package com.timgroup.eventstore.mysql

import java.sql.{ResultSet, Connection}

import com.timgroup.eventstore.api._
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.control.Exception.allCatch

import org.slf4j.LoggerFactory

trait ConnectionProvider {
  def getConnection(): Connection
}

trait EventPersister {
  def saveEventsToDB(connection: Connection, newEvents: Seq[EventAtATime], expectedVersion: Option[Long] = None): Unit
}

case class EventAtATime(effectiveTimestamp: DateTime, eventData: EventData)

object Utils extends CloseWithLogging {
  private val logger = LoggerFactory.getLogger(getClass)

  def transactionallyUsing[T](connectionProvider: ConnectionProvider)(code: Connection => T): T = {
    withResource(connectionProvider.getConnection()) { connection =>
      try {
        connection.setAutoCommit(false)
        val result = code(connection)
        connection.commit()
        result
      } catch {
        case e: Exception =>
          connection.rollback()
          throw e
      }
    }
  }

  def withResource[A <: AutoCloseable, B](open: => A)(usage: A => B): B = {
    val resource = open
    var throwing = false
    try {
      usage(resource)
    } catch {
      case ex: Throwable =>
        throwing = true
        try {
          resource.close()
        } catch {
          case rex: Throwable =>
            logger.info(s"Failure closing $resource", rex)
            ex.addSuppressed(rex)
        }
        throw ex
    } finally {
      if (!throwing) {
        closeWithLogging(resource)
      }
    }
  }
}

trait CloseWithLogging {
  private val logger = LoggerFactory.getLogger(getClass)

  protected[this] def closeWithLogging(c: AutoCloseable) = {
    (allCatch withTry {
      c.close()
    }).failed
      .foreach(logger.info(s"Failure closing", _))
  }
}
object CloseWithLogging {
  lazy val shouldLog = java.lang.Boolean.getBoolean("")
}

class SQLEventStore(connectionProvider: ConnectionProvider,
                    fetcher: SQLEventFetcher,
                    persister: EventPersister,
                    tableName: String,
                    now: () => DateTime = () => DateTime.now(DateTimeZone.UTC),
                    batchSize: Option[Int] = None) extends EventStore with CloseWithLogging {

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

    private def potentiallyFetchMore(): Unit = {
      if (!events.hasNext && hadNext) {
        events = fetchPage(currentVersion, batchSize).iterator
        hadNext = events.hasNext
      }
    }
  }

  override def fromAll(version: Long, eventHandler: EventInStream => Unit): Unit = {
    import Utils.withResource

    withResource(connectionProvider.getConnection()) { connection =>
      connection.setAutoCommit(false)
      withResource(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) { statement =>
        statement.setFetchSize(Integer.MIN_VALUE)
        withResource(statement.executeQuery("select effective_timestamp, eventType, body, version from  %s where version > %s".format(tableName, version))) { results =>
          while (results.next()) {
            val nextEvent = EventInStream(
              new DateTime(results.getTimestamp("effective_timestamp"), DateTimeZone.UTC),
              EventData(
                results.getString("eventType"),
                results.getBytes("body")),
              results.getLong("version")
            )
            eventHandler(nextEvent)
          }
        }
      }
    }
  }
}