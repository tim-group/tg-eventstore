package com.timgroup.mysqleventstore

import java.io.ByteArrayInputStream
import java.sql.{ResultSet, PreparedStatement, Timestamp, Connection}

import org.joda.time.{DateTimeZone, DateTime}

trait EventStore {
  def save(newEvents: Seq[EventData]): Unit

  def fromAll(version: Long = 0, batchSize: Option[Int] = None): EventPage
}

case class EventPage(events: Iterator[EventInStream]) {
  def eventData: Iterator[EventData] = events.map(_.eventData)

  def isEmpty = events.isEmpty
}

case class EventData(eventType: String, body: Array[Byte])

case class EventInStream(effectiveTimestamp: DateTime,
                         eventData: EventData,
                         version: Long,
                         lastVersion: Long) {
  val last = version == lastVersion
}

class SQLEventStore(tableName: String = "Event", now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) {

  def save(connection: Connection, newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit = {
    val effectiveTimestamp = now()
    saveRaw(connection, newEvents.map(EventInStream(effectiveTimestamp, _, 0, 0)), expectedVersion)
  }

  def saveRaw(connection: Connection, newEvents: Seq[EventInStream], expectedVersion: Option[Long] = None): Unit = {
    val statement = connection.prepareStatement("insert ignore into " + tableName + "(eventType,body,effective_timestamp,version) values(?,?,?,?)")

    val currentVersion = fetchCurrentVersion(connection)

    if (expectedVersion.map(_ != currentVersion).getOrElse(false)) {
      throw new OptimisticConcurrencyFailure()
    }

    try {
      newEvents.zipWithIndex.foreach {
        case (effectiveEvent, index) => {
          statement.clearParameters()
          statement.setString(1, effectiveEvent.eventData.eventType)
          statement.setBlob(2, new ByteArrayInputStream(effectiveEvent.eventData.body))
          statement.setTimestamp(3, new Timestamp(effectiveEvent.effectiveTimestamp.getMillis))
          statement.setLong(4, currentVersion + index + 1)
          statement.addBatch()
        }
      }

      val batches = statement.executeBatch()

      if (batches.size != newEvents.size) {
        throw new RuntimeException("We wrote " + batches.size + " but we were supposed to write: " + newEvents.size + " events")
      }

      if (batches.filter(_ != 1).nonEmpty) {
        throw new OptimisticConcurrencyFailure()
      }
    } finally {
      statement.close()
    }
  }

  private def fetchCurrentVersion(connection: Connection) = {
    val statement = connection.prepareStatement("select max(version) from " + tableName)
    val results = statement.executeQuery()

    try {
      results.next()
      results.getLong(1)
    } finally {
      results.close()
      statement.close()
    }
  }

  def fromAll(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): EventPage = {
    val statement = connection.prepareStatement("select effective_timestamp, eventType, body, version from  %s where version > ? %s".format(tableName, batchSize.map("limit " + _).getOrElse("")))
    statement.setLong(1, version)

    val last = fetchCurrentVersion(connection)

    val results = statement.executeQuery()

    try {
      val eventsIterator = new Iterator[EventInStream] {
        override def hasNext = results.next()

        override def next() = EventInStream(
          new DateTime(results.getTimestamp("effective_timestamp"),DateTimeZone.UTC),
          EventData(
            eventType = results.getString("eventType"),
            body = results.getBytes("body")),
          results.getLong("version"),
          last
        )
      }

      EventPage(eventsIterator.toList.toIterator)
    } finally {
      statement.close()
      results.close()
    }
  }
}