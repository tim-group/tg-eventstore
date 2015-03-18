package com.timgroup.mysqleventstore

import java.io.ByteArrayInputStream
import java.sql.{Connection, Timestamp}

import org.joda.time.{DateTime, DateTimeZone}

trait EventStore {
  def saveLiveEvent(newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit
  def saveBackfillEvent(newEvents: Seq[BackfillEvent], expectedVersion: Option[Long] = None): Unit
  def fromAll(version: Long = 0, batchSize: Option[Int] = None): EventStream
}

case class EventStream(effectiveEvents: Iterator[EffectiveEvent]) {
  def events: Iterator[EventData] = effectiveEvents.map(_.eventData)

  def isEmpty = effectiveEvents.isEmpty
}

case class EventData(eventType: String, body: Array[Byte])

case class BackfillEvent(effectiveTimestamp: DateTime,
                         version: Long,
                         eventData: EventData)


case class EffectiveEvent(effectiveTimestamp: DateTime,
                          eventData: EventData,
                          version: Long,
                          lastVersion: Long) {
  val last = version == lastVersion
}

class SQLEventStore(tableName: String = "Event", now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) { // extends EventStore

  private case class PersistableEvent(effectiveTimestamp: DateTime,
                                      eventData: EventData,
                                      version: Option[Long] = None)

  def saveLiveEvent(connection: Connection, newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit = {
    val effectiveTimestamp = now()
    saveEvent(connection, newEvents.map(PersistableEvent(effectiveTimestamp, _)), expectedVersion)
  }

  def saveBackfillEvent(connection: Connection, newEvents: Seq[BackfillEvent], expectedVersion: Option[Long] = None): Unit = {
    val persistableEvents = newEvents.map { backfillEvent =>
      PersistableEvent(backfillEvent.effectiveTimestamp, backfillEvent.eventData, Some(backfillEvent.version))
    }
    saveEvent(connection, persistableEvents, expectedVersion)
  }

  private def saveEvent(connection: Connection, newEvents: Seq[PersistableEvent], expectedVersion: Option[Long] = None): Unit = {
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

  def fromAll(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): EventStream = {
    val statement = connection.prepareStatement("select effective_timestamp, eventType, body, version from  %s where version > ? %s".format(tableName, batchSize.map("limit " + _).getOrElse("")))
    statement.setLong(1, version)

    val last = fetchCurrentVersion(connection)

    val results = statement.executeQuery()

    try {
      val eventsIterator = new Iterator[EffectiveEvent] {
        override def hasNext = results.next()

        override def next() = EffectiveEvent(
          new DateTime(results.getTimestamp("effective_timestamp"),DateTimeZone.UTC),
          EventData(
            eventType = results.getString("eventType"),
            body = results.getBytes("body")),
            results.getLong("version"),
            last
        )
      }

      EventStream(eventsIterator.toList.toIterator)
    } finally {
      statement.close()
      results.close()
    }
  }
}