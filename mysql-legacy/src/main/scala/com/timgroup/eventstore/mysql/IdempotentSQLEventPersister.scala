package com.timgroup.eventstore.mysql

import java.sql.{Connection, SQLException, Timestamp}

import com.timgroup.eventstore.api.{IdempotentWriteFailure, OptimisticConcurrencyFailure, CompatibilityPredicate}

import scala.util.control.NoStackTrace

/**
  * @deprecated uaw LegacyMysqlEventSource with an IdempotentEventStreamWriter instead
  */
@Deprecated
class IdempotentSQLEventPersister(tableName: String = "Event", lastVersionFetcher: LastVersionFetcher = new LastVersionFetcher("Event"),
                     compatibility: CompatibilityPredicate = CompatibilityPredicate.BytewiseEqual) extends EventPersister {

  private def _saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit = {
    val statement = connection.prepareStatement("insert into " + tableName + "(eventType,body,effective_timestamp,version) values(?,?,?,?)")

    try {
      newEvents.zipWithIndex.foreach {
        case (effectiveEvent, index) =>
          statement.clearParameters()
          statement.setString(1, effectiveEvent.eventData.eventType)
          statement.setBytes(2, effectiveEvent.eventData.body.data)
          statement.setTimestamp(3, new Timestamp(effectiveEvent.effectiveTimestamp.getMillis))
          statement.setLong(4, expectedVersion.getOrElse(0L) + index + 1)
          statement.addBatch()
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
      currentBatch.indices.foreach { i =>
        val version = fromVersion + i + 1
        if (!compatibility.test(version, currentBatch(i), newBatch(i).eventData)) {
          val currentBody = new String(currentBatch(i).body.data, "UTF-8")
          val newBody = new String(newBatch(i).eventData.body.data, "UTF-8")
          throw new IdempotentWriteFailure(s"event bodies do not match for version $version\n" +
                                           s"current: $currentBody\n" +
                                           s"    new: $newBody")
        }
      }
      val eventsAlreadyInDb = currentBatch.size
      val eventsNotYetInDb = newEvents.drop(eventsAlreadyInDb)
      _saveEventsToDB(connection, eventsNotYetInDb, Some(fromVersion + eventsAlreadyInDb))
    } else {
      if (lastVersion != fromVersion) {
        throw new OptimisticConcurrencyFailure(Some(new RuntimeException(s"lastVersion $lastVersion not equal to fromVersion $fromVersion, expectedVersion ${expectedVersion}, newEvents size ${newBatch.size}, currentBatch size ${currentBatch.size}") with NoStackTrace))
      }
      _saveEventsToDB(connection, newEvents, expectedVersion)
    }
  }
}
