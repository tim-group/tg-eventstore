package com.timgroup.mysqleventstore

import java.sql.{DriverManager, Connection}

import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{MustMatchers, FunSpec}

class SQLEventStoreTest extends FunSpec with MustMatchers {
  val effectiveTimestamp = new DateTime(2015, 1, 15, 23, 43, 53, DateTimeZone.UTC)

  case class ExampleEvent(a: Int)

  describe("writing to a stream") {
    it("creates and writes the first events, if the stream is new") {
      val eventStore = new SQLEventStore(now = () => effectiveTimestamp)

      inTransaction { conn =>
        eventStore.save(conn, serialized(ExampleEvent(21), ExampleEvent(22)))

        val all = eventStore.fromAll(conn).effectiveEvents.toList
        val lastTwo = all.slice(all.size-2, all.size)

        lastTwo.map(_.effectiveTimestamp) must be(List(effectiveTimestamp, effectiveTimestamp))
        lastTwo.map(_.event).map(deserialize) must be(List(ExampleEvent(21),ExampleEvent(22)))
      }
    }
  }

  it("can replay events from a given version number onwards") {
    val eventStore = new SQLEventStore()

    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2)))

      val previousVersion = eventStore.fromAll(conn).effectiveEvents.toList.last.version.get

      eventStore.save(conn, serialized(ExampleEvent(3), ExampleEvent(4)))

      val nextEvents = eventStore.fromAll(conn, version = previousVersion).effectiveEvents.toList.map(_.event).map(deserialize)

      nextEvents must be(List(ExampleEvent(3), ExampleEvent(4)))
    }
  }

  it("returns no events if there are none past the specified version") {
    val eventStore = new SQLEventStore()

    inTransaction { conn =>
      eventStore.fromAll(conn, version = 900000).isEmpty must be(true)
    }
  }

  it("labels the last event of the stream") {
    val eventStore = new SQLEventStore()

    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2)))

      val nextEvents = eventStore.fromAll(conn).effectiveEvents.toList

      nextEvents.map(_.last) must be(List(false, true))
    }
  }

  def serialized(evts: ExampleEvent*) = evts.map(serialize)

  def serialize(evt: ExampleEvent) = SerializedEvent(evt.getClass.getSimpleName, evt.a.toString.getBytes("UTF-8"))

  def deserialize(evt: SerializedEvent) = ExampleEvent(new String(evt.body, "UTF-8").toInt)

  def inTransaction[T](f: Connection => T): T = {
    val connection: Connection = connect()

    try {
      connection.setAutoCommit(false)
      f(connection)
    } finally {
      connection.rollback()
    }
  }

  def connect() = {
    DriverManager.registerDriver(new com.mysql.jdbc.Driver())
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC")
  }
}