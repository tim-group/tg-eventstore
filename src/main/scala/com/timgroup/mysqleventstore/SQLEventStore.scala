package com.timgroup.mysqleventstore

import java.io.ByteArrayInputStream
import java.sql.{Connection, Timestamp}

import org.joda.time.{DateTime, DateTimeZone}

import scala.util.control.Exception.allCatch

case class EventAtAtime(effectiveTimestamp: DateTime, eventData: EventData)

trait ConnectionProvider {
  def getConnection(): Connection
}

class SQLEventStore(connectionProvider: ConnectionProvider,
                    tableName: String = "Event",
                    now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) extends EventStore {
  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = {
    val connection = connectionProvider.getConnection()
    try {
      connection.setAutoCommit(false)
      val effectiveTimestamp = now()
      saveEventsToDB(connection, newEvents.map(EventAtAtime(effectiveTimestamp, _)), expectedVersion)
      connection.commit()
    } catch {
      case e: Exception => {
        connection.rollback()
        throw e
      }
    } finally {
      allCatch opt { connection.close() }
    }
  }

  override def fromAll(version: Long, batchSize: Option[Int]): EventPage = {
    val connection = connectionProvider.getConnection()
    try {
      fetchEventsFromDB(connection, version, batchSize)
    } finally {
      allCatch opt { connection.close() }
    }
  }

  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtAtime], expectedVersion: Option[Long] = None): Unit = {
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

  def fetchEventsFromDB(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): EventPage = {
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
}