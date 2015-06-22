package com.timgroup.eventstore.mysql

import java.sql.{Connection, DriverManager}
import com.timgroup.eventstore.api.EventStoreTest
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class SQLEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
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

  val eventStore = new SQLEventStore(connectionProvider, "Event", () => effectiveTimestamp, None)

  it should behave like anEventStore(eventStore)

  it should behave like optimisticConcurrencyControl(eventStore)
}