package com.timgroup.eventstore.mysql

import java.sql.{DriverManager, Connection}

import com.timgroup.eventstore.api._
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, MustMatchers, FunSpec, FunSuite}

class SQLEventPersisterTest extends FunSpec with MustMatchers with BeforeAndAfterEach {
  private val connectionProvider = new ConnectionProvider {
    override def getConnection(): Connection = {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver())
      DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC")
    }
  }

  override protected def afterEach(): Unit = {
    val conn = connectionProvider.getConnection()
    conn.prepareStatement("delete from Event").execute()
    conn.close()
  }

  it("throws OptimisticConcurrencyFailure when stream moves past expected version during save") {
    val connection = connectionProvider.getConnection()
    try {
      connection.setAutoCommit(false)

      val versionFetcherTriggeringStaleness = new LastVersionFetcher("Event") {
        override def fetchCurrentVersion(connection: Connection): Long = {
          val version = super.fetchCurrentVersion(connection)

          new SQLEventStore(connectionProvider, "Event", SystemClock).save(Seq(EventData("Event", Body(Array[Byte]()))))

          version
        }
      }
      val persister = new SQLEventPersister("Event", versionFetcherTriggeringStaleness)

      intercept[OptimisticConcurrencyFailure] {
        persister.saveEventsToDB(connection, Seq(EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte]())))))
      }
    } finally {
      connection.close()
    }
  }

  it("throws IdempotentWriteFailure when we write different stuff with the same version") {
    val connection = connectionProvider.getConnection()
    try {
      connection.setAutoCommit(false)

      val persister = new IdempotentSQLEventPersister("Event")

      intercept[IdempotentWriteFailure] {
        persister.saveEventsToDB(connection, Seq(EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte]())))), None)
        persister.saveEventsToDB(connection, Seq(EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](1))))), None)
      }
    } finally {
      connection.close()
    }
  }

  it("Idempotent Write allowed if the second write overlaps with the first") {

    Template.exec { case (persister, connection) =>
       persister.saveEventsToDB(connection,
          Seq(
            EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](1)))),
            EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](2))))
          ), Some(0L))

        persister.saveEventsToDB(connection,
          Seq(
            EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](2))))
          ), Some(2L))
      }
  }

  it("Idempotent Write base case") {

    Template.exec { case (persister, connection) =>
        persister.saveEventsToDB(connection,
          Seq(
            EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](1)))),
            EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](2))))
          ), Some(0L))

        persister.saveEventsToDB(connection,
          Seq(
            EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](1)))),
            EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte](2))))
          ), Some(0L))
      }
  }



  it("allows idempotent writes when the same stuff is written with the same version") {
    val connection = connectionProvider.getConnection()
    try {
      connection.setAutoCommit(false)

      val persister = new IdempotentSQLEventPersister("Event")

      persister.saveEventsToDB(connection, Seq(EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte]())))), None)
      persister.saveEventsToDB(connection, Seq(EventAtATime(new DateTime(), EventData("Event", Body(Array[Byte]())))), None)
    } finally {
      connection.close()
    }
  }

  object Template {

    def exec(f: (IdempotentSQLEventPersister, Connection) => Unit): Unit = {
      val connection = connectionProvider.getConnection()
      try {
        connection.setAutoCommit(false)
        val persister = new IdempotentSQLEventPersister("Event")
        f(persister, connection)
      } finally {
        connection.close()
      }
    }

  }

}
