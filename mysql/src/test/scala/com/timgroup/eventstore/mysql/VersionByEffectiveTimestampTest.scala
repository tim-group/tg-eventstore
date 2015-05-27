package com.timgroup.eventstore.mysql

import java.sql.{Connection, DriverManager}

import com.timgroup.eventstore.api.EventData
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class VersionByEffectiveTimestampTest extends FunSpec with MustMatchers with BeforeAndAfterEach {
  private val connectionProvider = new ConnectionProvider {
    override def getConnection(): Connection = {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver())
      DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC")
    }
  }

  val beforeCuttoff = new DateTime(2014, 5, 4, 12, 54, 21, UTC)
  val cuttoff = beforeCuttoff.plusDays(5)
  val afterCuttoff = cuttoff.plusDays(5)

  it("fetches the version before the first event after the specified effective timestamp cuttoff") {
    val clock = new FixedClock
    val eventStore = SQLEventStore(connectionProvider, now = clock)

    clock.currentTimeIs(beforeCuttoff)
    eventStore.save(Seq(EventData("Blah", "{}".getBytes("utf-8"))))
    eventStore.save(Seq(EventData("Blah", "{}".getBytes("utf-8"))))
    clock.currentTimeIs(afterCuttoff)
    eventStore.save(Seq(EventData("Blah", "{}".getBytes("utf-8"))))

    new VersionByEffectiveTimestamp(connectionProvider).versionFor(cuttoff) must be(2)
  }

  it("returns zero when there are no events") {
    new VersionByEffectiveTimestamp(connectionProvider).versionFor(cuttoff) must be(0)
  }

  it("if there are no new events after the cuttoff returns the last eventversion") {
    val clock = new FixedClock
    val eventStore = SQLEventStore(connectionProvider, now = clock)

    clock.currentTimeIs(beforeCuttoff)
    eventStore.save(Seq(EventData("Blah", "{}".getBytes("utf-8"))))
    eventStore.save(Seq(EventData("Blah", "{}".getBytes("utf-8"))))
    eventStore.save(Seq(EventData("Blah", "{}".getBytes("utf-8"))))

    new VersionByEffectiveTimestamp(connectionProvider).versionFor(cuttoff) must be(3)
  }

  override protected def afterEach(): Unit = {
    val conn = connectionProvider.getConnection()
    conn.prepareStatement("delete from Event").execute()
    conn.close()
  }
}

class FixedClock() extends (() => DateTime) {
  private var time: DateTime = null

  def currentTimeIs(dateTime: DateTime): Unit = {
    time = dateTime
  }

  override def apply(): DateTime = time
}
