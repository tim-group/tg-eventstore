package com.timgroup.eventstore.mysql.legacy

import java.sql.{Connection, DriverManager}
import com.timgroup.eventstore.api.EventStoreTest
import com.timgroup.eventstore.mysql.ConnectionProvider
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class AutoIncrementBasedEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
  private val connectionProvider = new ConnectionProvider {
    override def getConnection(): Connection = {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver())
      DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC")
    }
  }

  override protected def beforeEach(): Unit = {
    val conn = connectionProvider.getConnection()
    conn.prepareStatement("DROP TABLE IF EXISTS AutoIncrementEvent").execute()
    conn.prepareStatement("CREATE TABLE AutoIncrementEvent(eventType VARCHAR(255), body BLOB, version INT PRIMARY KEY AUTO_INCREMENT, effective_timestamp datetime)").execute()
    conn.close()
  }

  val eventStore = AutoIncrementBasedEventStore(
    connectionProvider,
    now = () => effectiveTimestamp,
    tableName = "AutoIncrementEvent"
  )

  it should behave like anEventStore(eventStore)
}