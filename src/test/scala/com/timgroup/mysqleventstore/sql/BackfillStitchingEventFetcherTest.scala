package com.timgroup.mysqleventstore.sql

import java.sql.{DriverManager, Connection}

import com.timgroup.mysqleventstore.{Body, EventInStream, EventData, EventStoreTest}
import com.timgroup.mysqleventstore.sql.legacy.AutoIncrementBasedEventPersister
import org.joda.time.DateTimeZone.UTC
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{BeforeAndAfterEach, MustMatchers, FunSpec, FunSuite}

import scala.util.Random

class BackfillStitchingEventFetcherTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
  val emtpyBody = Body(Array())
  private val connectionProvider = new ConnectionProvider {
    override def getConnection(): Connection = {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver())
      DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC")
    }
  }

  override protected def beforeEach(): Unit = {
    val conn = connectionProvider.getConnection()
    conn.prepareStatement("DROP TABLE IF EXISTS EventsLive").execute()
    conn.prepareStatement("DROP TABLE IF EXISTS EventsBackfill").execute()
    conn.prepareStatement("CREATE TABLE EventsLive(eventType VARCHAR(255), body BLOB, version INT PRIMARY KEY AUTO_INCREMENT, effective_timestamp datetime) AUTO_INCREMENT=1000").execute()
    conn.prepareStatement("CREATE TABLE EventsBackfill(eventType VARCHAR(255), body BLOB, version INT PRIMARY KEY AUTO_INCREMENT, effective_timestamp datetime)").execute()
    conn.close()
  }

  val effectiveTime = new DateTime(2015, 12, 12, 12, 12,UTC)

  val backfillPersister = new AutoIncrementBasedEventPersister("EventsBackfill")

  val eventStore = new SQLEventStore(
    connectionProvider,
    new BackfillStitchingEventFetcher(new SQLEventFetcher("EventsBackfill"), new SQLEventFetcher("EventsLive")),
    new AutoIncrementBasedEventPersister("EventsLive"),
    now = () => effectiveTime)

  it("fetches all events from both tables when no batch size specified") {
    val backfillA = EventData("A", randomContents())
    val backfillB = EventData("B", randomContents())
    val liveA = EventData("A", randomContents())
    val liveB = EventData("B", randomContents())

    saveInBackfill(Seq(
      backfillA,
      backfillB
    ))

    eventStore.save(Seq(
      liveA,
      liveB
    ))

    eventStore.fromAll().events.toList must be(List(
      EventInStream(effectiveTime, backfillA, 1, 1001),
      EventInStream(effectiveTime, backfillB, 2, 1001),
      EventInStream(effectiveTime, liveA, 1000, 1001),
      EventInStream(effectiveTime, liveB, 1001, 1001)
    ))
  }

  it("fetches batch from just the backfill if enough events are present") {
    saveInBackfill(Seq(
      EventData("Backfill", randomContents()),
      EventData("Backfill", randomContents()),
      EventData("Backfill", randomContents())
    ))

    eventStore.save(Seq(
      EventData("Live", randomContents()),
      EventData("Live", randomContents()),
      EventData("Live", randomContents())
    ))

    eventStore.fromAll(batchSize = Some(2)).eventData.map(_.eventType).toList must be(List("Backfill", "Backfill"))
  }

  it("fetches an overlapping batch if required") {
    saveInBackfill(Seq(
      EventData("Backfill", randomContents()),
      EventData("Backfill", randomContents()),
      EventData("Backfill", randomContents())
    ))

    eventStore.save(Seq(
      EventData("Live", randomContents()),
      EventData("Live", randomContents()),
      EventData("Live", randomContents())
    ))

    eventStore.fromAll(batchSize = Some(5)).eventData.map(_.eventType).toList must be(List(
      "Backfill",
      "Backfill",
      "Backfill",
      "Live",
      "Live"
    ))
  }

  it("fetches data from backfill table if live table is empty") {
    saveInBackfill(Seq(
      EventData("Backfill", randomContents()),
      EventData("Backfill", randomContents()),
      EventData("Backfill", randomContents())
    ))

    eventStore.fromAll().events.map(evt => (evt.version, evt.lastVersion)).toList must be(List(
      (1, 3),
      (2, 3),
      (3, 3)
    ))
  }

  def randomContents() = Random.nextString(10).getBytes("utf-8")

  def saveInBackfill(evts: Seq[EventData]) = {
    val conn = connectionProvider.getConnection()
    try {
      backfillPersister.saveEventsToDB(conn, evts.map(EventAtATime(effectiveTime, _)))
    } finally {
      conn.close()
    }
  }
}
