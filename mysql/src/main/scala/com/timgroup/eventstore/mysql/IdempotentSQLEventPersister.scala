package com.timgroup.eventstore.mysql

import java.sql.{Connection, SQLException, Timestamp}

import com.timgroup.eventstore.api.{IdempotentWriteFailure, OptimisticConcurrencyFailure}

class IdempotentSQLEventPersister(tableName: String = "Event", lastVersionFetcher: LastVersionFetcher = new LastVersionFetcher("Event")) extends EventPersister {

  def _saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit = {
    val statement = connection.prepareStatement("insert into " + tableName + "(eventType,body,effective_timestamp,version) values(?,?,?,?)")

    try {
      newEvents.zipWithIndex.foreach {
        case (effectiveEvent, index) => {
          statement.clearParameters()
          statement.setString(1, effectiveEvent.eventData.eventType)
          statement.setBytes(2, effectiveEvent.eventData.body.data)
          statement.setTimestamp(3, new Timestamp(effectiveEvent.effectiveTimestamp.getMillis))
          statement.setLong(4, expectedVersion.getOrElse(0L) + index + 1)
          statement.addBatch()
        }
      }

      val batches = statement.executeBatch()

      if (batches.size != newEvents.size) {
        throw new RuntimeException("We wrote " + batches.size + " but we were supposed to write: " + newEvents.size + " events")
      }
    } catch {
      case e: SQLException if e.getMessage.contains("Duplicate") => throw new OptimisticConcurrencyFailure(None)
    } finally {
      statement.close()
    }
  }


  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit = {
    val fromVersion = expectedVersion.getOrElse(0L)
    val (lastVersion, currentBatch) = lastVersionFetcher.fetchBatch(connection, fromVersion, newEvents.size)
    val newBatch = newEvents.toVector

    if (currentBatch.nonEmpty) {
      if (currentBatch.size != newBatch.size) {
        throw new IdempotentWriteFailure("batch sizes must match")
      }

      currentBatch.indices.foreach { i =>
        if (currentBatch(i).body != newBatch(i).eventData.body) {
          val version = fromVersion + i
          val currentBody = new String(currentBatch(i).body.data, "UTF-8")
          val newBody = new String(newBatch(i).eventData.body.data, "UTF-8")
          throw new IdempotentWriteFailure(s"event bodies do not match for version $version\n" +
                                           s"current: $currentBody\n" +
                                           s"    new: $newBody")
        }
      }
    } else {
      if (lastVersion != fromVersion) {
        throw new OptimisticConcurrencyFailure(None)
      }
      _saveEventsToDB(connection, newEvents, expectedVersion)
    }
  }
}