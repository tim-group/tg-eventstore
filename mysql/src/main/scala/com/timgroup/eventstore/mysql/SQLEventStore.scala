package com.timgroup.eventstore.mysql

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
            now: () => DateTime = () => DateTime.now(DateTimeZone.UTC),
            batchSize: Option[Int] = None) = {
    new SQLEventStore(
      connectionProvider,
      new SQLEventFetcher(tableName),
      new SQLEventPersister(tableName),
      now,
      batchSize
    )
  }
}

trait EventPersister {
  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit
}

trait EventFetcher {
  def fetchEventsFromDB(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): Seq[EventInStream]
}

case class EventPage(events: Seq[EventInStream], isLastPage: Boolean) {
  def eventData: Seq[EventData] = events.map(_.eventData)

  def isEmpty = events.isEmpty

  def size = events.size

  def lastOption = events.lastOption
}

case class EventAtATime(effectiveTimestamp: DateTime, eventData: EventData)

class SQLEventStore(connectionProvider: ConnectionProvider,
                    fetcher: EventFetcher,
                    persister: EventPersister,
                    now: () => DateTime = () => DateTime.now(DateTimeZone.UTC),
                    batchSize: Option[Int] = None) extends EventStore {
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

  private def fetchPage(version: Long, batchSize: Option[Int]) = {
    val connection = connectionProvider.getConnection()
    try {
      connection.setAutoCommit(false)
      fetcher.fetchEventsFromDB(connection, version, batchSize)
    } finally {
      allCatch opt { connection.rollback() }
      allCatch opt { connection.close() }
    }
  }

  override def fromAll(version: Long): EventStream = new EventStream {
    private var events: Iterator[EventInStream] = Iterator.empty
    private var currentVersion = version

    override def next(): EventInStream = {
      potentiallyFetchMore()
      val event = events.next()
      currentVersion = event.version
      event
    }

    override def hasNext: Boolean = {
      potentiallyFetchMore()
      events.hasNext
    }

    private def potentiallyFetchMore(): Unit = {
      if (!events.hasNext) {
        events = fetchPage(currentVersion, batchSize).iterator
      }
    }
  }
}