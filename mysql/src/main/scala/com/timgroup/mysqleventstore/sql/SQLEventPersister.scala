package com.timgroup.mysqleventstore.sql

import java.io.ByteArrayInputStream
import java.sql.{Connection, Timestamp}

import com.timgroup.eventstore.api.{OptimisticConcurrencyFailure, EventAtATime}

class SQLEventPersister(tableName: String = "Event", fetcher: SQLHeadVersionFetcher) extends EventPersister {
  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit = {
    val statement = connection.prepareStatement("insert ignore into " + tableName + "(eventType,body,effective_timestamp,version) values(?,?,?,?)")

    val currentVersion = fetcher.fetchCurrentVersion(connection)

    if (expectedVersion.map(_ != currentVersion).getOrElse(false)) {
      throw new OptimisticConcurrencyFailure()
    }

    try {
      newEvents.zipWithIndex.foreach {
        case (effectiveEvent, index) => {
          statement.clearParameters()
          statement.setString(1, effectiveEvent.eventData.eventType)
          statement.setBlob(2, new ByteArrayInputStream(effectiveEvent.eventData.body.data))
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
}
