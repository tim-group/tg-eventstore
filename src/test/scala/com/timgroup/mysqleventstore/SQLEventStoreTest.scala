package com.timgroup.mysqleventstore

import java.sql.{DriverManager, Connection}

import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{MustMatchers, FunSpec}

class SQLEventStoreTest extends FunSpec with MustMatchers {
  val effectiveTimestamp = new DateTime(2015, 1, 15, 23, 43, 53, DateTimeZone.UTC)

  val eventStore = new SQLEventStore(now = () => effectiveTimestamp)

  it("can save events") {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(21), ExampleEvent(22)))

      val all = eventStore.fromAll(conn).effectiveEvents.toList

      all.map(_.effectiveTimestamp) must be(List(effectiveTimestamp, effectiveTimestamp))
      all.map(_.event).map(deserialize) must be(List(ExampleEvent(21),ExampleEvent(22)))
    }
  }

  it("can replay events from a given version number onwards") {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2)))

      val previousVersion = eventStore.fromAll(conn).effectiveEvents.toList.last.version.get

      eventStore.save(conn, serialized(ExampleEvent(3), ExampleEvent(4)))

      val nextEvents = eventStore.fromAll(conn, version = previousVersion).effectiveEvents.toList.map(_.event).map(deserialize)

      nextEvents must be(List(ExampleEvent(3), ExampleEvent(4)))
    }
  }

  it("returns no events if there are none past the specified version") {
    inTransaction { conn =>
      eventStore.fromAll(conn, version = 900000).isEmpty must be(true)
    }
  }

  it("labels the last event of the stream") {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2)))

      val nextEvents = eventStore.fromAll(conn).effectiveEvents.toList

      nextEvents.map(_.last) must be(List(false, true))
    }
  }

  it("writes events to the current version of the stream when no expected version is specified") {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2)))
      eventStore.save(conn, serialized(ExampleEvent(3)))

      eventStore.fromAll(conn, version = 2).effectiveEvents.toList.map(_.event).map(deserialize) must be(List(ExampleEvent(3)))
    }
  }

  it("throws OptimisticConcurrencyFailure when stream has already moved beyond the expected version") {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2)))
      intercept[OptimisticConcurrencyFailure] {
        eventStore.save(conn, serialized(ExampleEvent(3)), expectedVersion = Some(1))
      }
    }
  }

  it("throws OptimisticConcurrencyFailure when stream is not yet at the expected version") {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2)))
      intercept[OptimisticConcurrencyFailure] {
        eventStore.save(conn, serialized(ExampleEvent(3)), expectedVersion = Some(10))
      }
    }
  }

  it("throws OptimisticConcurrencyFailure when stream moves past expected version during save") {
    try {
      inTransaction { conn =>
        eventStore.fromAll(conn)

        unrelatedSavesOfEventHappens()
        intercept[OptimisticConcurrencyFailure] {
          eventStore.save(conn, serialized(ExampleEvent(3)), expectedVersion = Some(0))
        }
      }
    } finally {
      cleanup()
    }
  }

  it("returns nothing if eventstore is empty") {
    inTransaction { conn =>
      eventStore.fromAll(conn).events.toList must be(Nil)
    }
  }


  it("returns only number of events asked for in batchSize") {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(1), ExampleEvent(2), ExampleEvent(3), ExampleEvent(4)))

      eventStore.fromAll(conn, 0, Some(2)).effectiveEvents.toList.map(_.event).map(deserialize) must be(List(ExampleEvent(1), ExampleEvent(2)))
      eventStore.fromAll(conn, 2, Some(2)).effectiveEvents.toList.map(_.event).map(deserialize) must be(List(ExampleEvent(3), ExampleEvent(4)))
    }
  }

  def cleanup(): Unit = {
    inTransaction { conn =>
      conn.prepareStatement("delete from Event").execute()
      conn.commit()
    }
  }

  def unrelatedSavesOfEventHappens(): Unit = {
    inTransaction { conn =>
      eventStore.save(conn, serialized(ExampleEvent(3)))
      conn.commit()
    }
  }

  case class ExampleEvent(a: Int)

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