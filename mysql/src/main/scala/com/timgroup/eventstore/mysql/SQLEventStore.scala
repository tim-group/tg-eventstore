package com.timgroup.eventstore.mysql

import java.sql.{Connection, ResultSet, Statement}
import java.util.Spliterator
import java.util.function.Consumer
import java.util.stream.{Stream, StreamSupport}

import com.timgroup.eventstore.api._
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.control.Exception.allCatch

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
trait EventPersister {
  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit
}

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
case class EventAtATime(effectiveTimestamp: DateTime, eventData: EventData)

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
class SQLEventStore(connectionProvider: ConnectionProvider,
                    fetcher: SQLEventFetcher,
                    persister: EventPersister,
                    tableName: String,
                    now: () => DateTime = () => DateTime.now(DateTimeZone.UTC),
                    batchSize: Option[Int] = None) extends EventStore {

  def this(connectionProvider: ConnectionProvider,
           tableName: String,
           now: () => DateTime,
           batchSize: Option[Int]) {
    this(connectionProvider,
         new SQLEventFetcher(tableName),
         new SQLEventPersister(tableName, new LastVersionFetcher(tableName)),
         tableName,
         now,
         batchSize)
  }
  def this(connectionProvider: ConnectionProvider,
           tableName: String,
           clock: Clock) {
    this(connectionProvider, tableName, () => clock.now(), None)
  }

  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = {
    ResourceManagement.transactionallyUsing(connectionProvider) { connection =>
      val effectiveTimestamp = now()
      persister.saveEventsToDB(connection, newEvents.map(EventAtATime(effectiveTimestamp, _)), expectedVersion)
    }
  }

  private def fetchPage(version: Long, batchSize: Option[Int]) = ResourceManagement.transactionallyUsing(connectionProvider) { connection =>
    fetcher.fetchEventsFromDB(connection, version, batchSize)
  }

  override def fromAll(version: Long) = new EventStream {
    private var events = Iterator.empty
    private var currentVersion = version
    private var hadNext = true

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

    private def potentiallyFetchMore() = {
      if (!events.hasNext && hadNext) {
        events = fetchPage(currentVersion, batchSize).iterator
        hadNext = events.hasNext
      }
    }
  }


  override def streamingFromAll(version: Long): Stream[EventInStream] = {
    val connection = connectionProvider.getConnection()
    var statement = null
    var resultSet = null

    val closeConnection = new Runnable {
      override def run(): Unit = {
        allCatch { resultSet.close() }
        allCatch { statement.close() }
        allCatch { connection.close() }
      }
    }

    try
      connection.setAutoCommit(false)
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      statement.setFetchSize(Integer.MIN_VALUE)
      resultSet = statement.executeQuery("select effective_timestamp, eventType, body, version from  %s where version > %s".format(tableName, version))

      StreamSupport.stream(new Spliterator[EventInStream] {
        override def estimateSize() = Long.MaxValue

        override def tryAdvance(action: Consumer[_ >: EventInStream]): Boolean = if (resultSet.next()) {
          action.accept(EventInStream(
            new DateTime(resultSet.getTimestamp("effective_timestamp"), DateTimeZone.UTC),
            EventData(
              resultSet.getString("eventType"),
              resultSet.getBytes("body")),
            resultSet.getLong("version")
          ))
          true
        } else {
          closeConnection.run()
          false
        }

        override def trySplit() = null

        override def characteristics() = Spliterator.ORDERED
      }, false).onClose(closeConnection);
    catch {
      case e: Exception => {
        closeConnection.run()
        throw e
      }
    }
  }
}