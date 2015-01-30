package com.timgroup.mysqleventstore

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import java.sql.{DriverManager, Connection}

import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{MustMatchers, FunSpec}

class SQLEventStoreFunctionalSpec extends FunSpec with MustMatchers {
  val effectiveTimestamp = new DateTime(2015, 1, 15, 23, 43, 53, DateTimeZone.UTC)

  case class ExampleEvent(a: Int)

  describe("writing to a stream") {
    it("creates and writes the first events, if the stream is new") {
      val eventStore = new SQLEventStore(now = () => effectiveTimestamp)

      inTransaction { conn =>
        eventStore.save(conn, Seq(ExampleEvent(21), ExampleEvent(22)).map(serialize))

        val all = eventStore.fromAll(conn).effectiveEvents.toList
        val lastTwo = all.slice(all.size-2, all.size)

        lastTwo.map(_.effectiveTimestamp) must be(List(effectiveTimestamp, effectiveTimestamp))
        lastTwo.map(_.event).map(deserialize) must be(List(ExampleEvent(21),ExampleEvent(22)))
      }
    }
  }

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