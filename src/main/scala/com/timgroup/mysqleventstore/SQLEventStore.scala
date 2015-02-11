package com.timgroup.mysqleventstore

import java.io.ByteArrayInputStream
import java.sql.{ResultSet, PreparedStatement, Timestamp, Connection}

import org.joda.time.{DateTimeZone, DateTime}

trait EventStore {
  def save(newEvents: Seq[SerializedEvent]): Unit

  def fromAll(version: Long = 0): EventStream
}

case class EventStream(effectiveEvents: Iterator[EffectiveEvent]) {
  def events: Iterator[SerializedEvent] = effectiveEvents.map(_.event)

  def isEmpty = effectiveEvents.isEmpty
}

case class SerializedEvent(eventType: String, body: Array[Byte])

case class EffectiveEvent(
                           effectiveTimestamp: DateTime,
                           event: SerializedEvent,
                           version: Option[Long] = None,
                           lastVersion: Option[Long] = None
                           ) {
  val last = (for {
    me <- version
    head <- lastVersion
  } yield (me == head)).getOrElse(false)
}

class SQLEventStore(tableName: String = "Event", now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) {

  def save(connection: Connection, newEvents: Seq[SerializedEvent], expectedVersion: Option[Long] = None): Unit = {
    val effectiveTimestamp = now()
    saveRaw(connection, newEvents.map(EffectiveEvent(effectiveTimestamp, _)), expectedVersion)
  }

  def saveRaw(connection: Connection, newEvents: Seq[EffectiveEvent], expectedVersion: Option[Long] = None): Unit = {
    val statement = connection.prepareStatement("insert ignore into " + tableName + "(eventType,body,effective_timestamp,version) values(?,?,?,?)")

    val currentVersion = fetchCurrentVersion(connection)

    if (expectedVersion.map(_ != currentVersion).getOrElse(false)) {
      throw new OptimisticConcurrencyFailure()
    }

    try {
      newEvents.zipWithIndex.foreach {
        case (effectiveEvent, index) => {
          statement.clearParameters()
          statement.setString(1, effectiveEvent.event.eventType)
          statement.setBlob(2, new ByteArrayInputStream(effectiveEvent.event.body))
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

  def fromAll(connection: Connection, version: Long = 0): EventStream = {
    val statement = connection.prepareStatement("select effective_timestamp, eventType, body, version from  %s where version > ?".format(tableName))
    statement.setLong(1, version)

    val last = fetchCurrentVersion(connection)

    val results = statement.executeQuery()

    try {
      val eventsIterator = new Iterator[EffectiveEvent] {
        override def hasNext = results.next()

        override def next() = EffectiveEvent(
          new DateTime(results.getTimestamp("effective_timestamp"),DateTimeZone.UTC),
          SerializedEvent(
            eventType = results.getString("eventType"),
            body = results.getBytes("body")),
          Some(results.getLong("version")),
          Some(last)
        )
      }

      EventStream(eventsIterator.toList.toIterator)
    } finally {
      statement.close()
      results.close()
    }
  }
}