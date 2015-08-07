package com.timgroup.eventstore.mysql

import java.sql.{SQLException, Connection, Timestamp}

import com.timgroup.eventstore.api.OptimisticConcurrencyFailure

import scala.util.control.Exception._

class SQLEventPersister(tableName: String = "Event", lastVersionFetcher: LastVersionFetcher = new LastVersionFetcher("Event")) extends EventPersister {
  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit = {
    val statement = connection.prepareStatement("insert into " + tableName + "(eventType,body,effective_timestamp,version) values(?,?,?,?)")

    val currentVersion = lastVersionFetcher.fetchCurrentVersion(connection)

    if (expectedVersion.map(_ != currentVersion).getOrElse(false)) {
      throw new OptimisticConcurrencyFailure()
    }

    try {
      newEvents.zipWithIndex.foreach {
        case (effectiveEvent, index) => {
          statement.clearParameters()
          statement.setString(1, effectiveEvent.eventData.eventType)
          statement.setBytes(2, effectiveEvent.eventData.body.data)
          statement.setTimestamp(3, new Timestamp(effectiveEvent.effectiveTimestamp.getMillis))
          statement.setLong(4, currentVersion + index + 1)
          statement.addBatch()
        }
      }

      val batches = statement.executeBatch()

      if (batches.size != newEvents.size) {
        throw new RuntimeException("We wrote " + batches.size + " but we were supposed to write: " + newEvents.size + " events")
      }
    } catch {
      case e: SQLException if e.getMessage.contains("Duplicate") => throw new OptimisticConcurrencyFailure()
    } finally {
      statement.close()
    }
  }
}

class LastVersionFetcher(tableName: String = "Event") {
  def fetchCurrentVersion(connection: Connection): Long = {
    val statement = connection.prepareStatement("select max(version) from " + tableName)
    val results = statement.executeQuery()

    try {
      results.next()
      results.getLong(1)
    } finally {
      allCatch opt { results.close() }
      allCatch opt { statement.close() }
    }
  }
}
