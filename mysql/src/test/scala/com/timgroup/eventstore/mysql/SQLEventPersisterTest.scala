package com.timgroup.eventstore.mysql

import java.sql.{DriverManager, Connection}

import com.timgroup.eventstore.api.{OptimisticConcurrencyFailure, Body, EventData}
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

          SQLEventStore(connectionProvider).save(Seq(EventData("Event", Body(Array[Byte]()))))

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
}
