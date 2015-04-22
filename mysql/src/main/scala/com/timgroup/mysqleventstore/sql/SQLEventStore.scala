package com.timgroup.mysqleventstore.sql

import java.sql.Connection

import com.timgroup.eventstore.api._
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.control.Exception.allCatch

trait ConnectionProvider {
  def getConnection(): Connection
}

object SQLEventStore {
  def apply(connectionProvider: ConnectionProvider,
            tableName: String = "Event",
            now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) = {
    val headVersionFetcher = new SQLHeadVersionFetcher(tableName)

    new SQLEventStore(
      connectionProvider,
      new SQLEventFetcher(tableName),
      new SQLEventPersister(tableName, headVersionFetcher),
      headVersionFetcher,
      now)
  }
}

trait EventPersister {
  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit
}

trait EventFetcher {
  def fetchEventsFromDB(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): Seq[EventInStream]
}

trait HeadVersionFetcher {
  def fetchCurrentVersion(connection: Connection): Long
}

class SQLEventStore(connectionProvider: ConnectionProvider,
                    fetcher: EventFetcher,
                    persister: EventPersister,
                    headVersionFetcher: HeadVersionFetcher,
                    now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) extends EventStore {
  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = {
    val connection = connectionProvider.getConnection()
    try {
      connection.setAutoCommit(false)
      val effectiveTimestamp = now()
      persister.saveEventsToDB(connection, newEvents.map(EventAtATime(effectiveTimestamp, _)), expectedVersion)
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
      connection.setAutoCommit(false)
      EventPage(fetcher.fetchEventsFromDB(connection, version, batchSize), headVersionFetcher.fetchCurrentVersion(connection))
    } finally {
      allCatch opt { connection.rollback() }
      allCatch opt { connection.close() }
    }
  }
}