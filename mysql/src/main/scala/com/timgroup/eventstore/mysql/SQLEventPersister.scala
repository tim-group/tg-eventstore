package com.timgroup.eventstore.mysql

import java.sql.{Connection, SQLException, Timestamp}

import com.timgroup.eventstore.api.{EventData, OptimisticConcurrencyFailure}
import com.timgroup.eventstore.mysql.ResourceManagement.withResource

class SQLEventPersister(tableName: String = "Event", lastVersionFetcher: LastVersionFetcher = new LastVersionFetcher("Event")) extends EventPersister {

  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit = {
    withResource(connection.prepareStatement("insert into " + tableName + "(eventType,body,effective_timestamp,version) values(?,?,?,?)")) { statement =>
      val currentVersion = lastVersionFetcher.fetchCurrentVersion(connection)

      if (expectedVersion.exists(_ != currentVersion)) {
        throw new OptimisticConcurrencyFailure(None)
      }

      try {
        newEvents.zipWithIndex.foreach {
          case (effectiveEvent, index) =>
            statement.clearParameters()
            statement.setString(1, effectiveEvent.eventData.eventType)
            statement.setBytes(2, effectiveEvent.eventData.body.data)
            statement.setTimestamp(3, new Timestamp(effectiveEvent.effectiveTimestamp.getMillis))
            statement.setLong(4, currentVersion + index + 1)
            statement.addBatch()
        }

        val batches = statement.executeBatch()

        if (batches.size != newEvents.size) {
          throw new RuntimeException("We wrote " + batches.size + " but we were supposed to write: " + newEvents.size + " events")
        }
      } catch {
        case e: SQLException if e.getMessage.contains("Duplicate") => throw new OptimisticConcurrencyFailure(Some(e))
      }
    }
  }
}

class LastVersionFetcher(tableName: String = "Event") {
  def fetchBatch(connection: Connection, fromVersion: Long, batchsize: Int): (Long, Vector[EventData]) =
    withResource(connection.createStatement()) { statement =>
      withResource(statement.executeQuery(s"select version, eventType, body from ${tableName} where version>=${fromVersion} limit ${batchsize+1}")) { results =>
        var batch = Vector[EventData]()
        var lastVersion = 0L
        while(results.next()) {
          lastVersion = results.getLong("version")

          if (lastVersion > fromVersion && batch.size < batchsize) {
            batch = batch :+
              EventData(
                results.getString("eventType"),
                results.getBytes("body")
              )
          }

        }
        (lastVersion, batch)
      }
    }

  def fetchCurrentVersion(connection: Connection): Long =
    withResource(connection.prepareStatement("select max(version) from " + tableName)) { statement =>
      withResource(statement.executeQuery()) { results =>
        results.next()
        results.getLong(1)
      }
    }
}
