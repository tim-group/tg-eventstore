package com.timgroup.eventstore.mysql.legacy

import java.sql.{Connection, Timestamp}

import com.timgroup.clocks.joda.JodaClock
import com.timgroup.eventstore.mysql._

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
object AutoIncrementBasedEventStore {
  def apply(connectionProvider: ConnectionProvider,
            tableName: String = "Event",
            now: JodaClock = JodaClock.getDefault.withUTC(),
            batchSize: Option[Int] = None) = {
    new SQLEventStore(
      connectionProvider,
      new SQLEventFetcher(tableName),
      new AutoIncrementBasedEventPersister(tableName),
      tableName,
      now,
      batchSize)
  }
}
/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
class AutoIncrementBasedEventPersister(tableName: String) extends EventPersister {
  override def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long]): Unit = {
    if (expectedVersion.isDefined) {
      throw new RuntimeException("This implementation does not support optimistic concurrency.")
    }
    val statement = connection.prepareStatement("insert into " + tableName + "(eventType,body,effective_timestamp) values(?,?,?)")

    try {
      newEvents.foreach { effectiveEvent =>
        statement.clearParameters()
        statement.setString(1, effectiveEvent.eventData.eventType)
        statement.setBytes(2, effectiveEvent.eventData.body.data)
        statement.setTimestamp(3, new Timestamp(effectiveEvent.effectiveTimestamp.getMillis))
        statement.addBatch()
      }

      val batches = statement.executeBatch()

      if (batches.size != newEvents.size) {
        throw new RuntimeException("We wrote " + batches.size + " but we were supposed to write: " + newEvents.size + " events")
      }
    } finally {
      statement.close()
    }
  }
}
