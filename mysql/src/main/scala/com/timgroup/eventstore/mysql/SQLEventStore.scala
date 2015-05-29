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

case class EventAtATime(effectiveTimestamp: DateTime, eventData: EventData)

object Utils {
  def transactionallyUsing[T](connectionProvider: ConnectionProvider)(code: Connection => T): T = {
    val connection = connectionProvider.getConnection()

    try {
      connection.setAutoCommit(false)
      val result = code(connection)
      connection.commit()
      return result
    } catch {
      case e: Exception => {
        connection.rollback()
        throw e
      }
    } finally {
      allCatch opt { connection.close() }
    }
  }
}

class SQLEventStore(connectionProvider: ConnectionProvider,
                    fetcher: SQLEventFetcher,
                    persister: EventPersister,
                    now: () => DateTime = () => DateTime.now(DateTimeZone.UTC),
                    batchSize: Option[Int] = None) extends EventStore {
  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = {
    Utils.transactionallyUsing(connectionProvider) { connection =>
      val effectiveTimestamp = now()
      persister.saveEventsToDB(connection, newEvents.map(EventAtATime(effectiveTimestamp, _)), expectedVersion)
    }
  }

  private def fetchPage(version: Long, batchSize: Option[Int]) = {
    Utils.transactionallyUsing(connectionProvider) { connection =>
      fetcher.fetchEventsFromDB(connection, version, batchSize)
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