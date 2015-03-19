package com.timgroup.mysqleventstore

import java.lang.reflect.Proxy.newProxyInstance
import java.lang.reflect.{Method, InvocationHandler}
import java.sql.{Connection, DriverManager}

import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FunSpec, MustMatchers}

class SQLEventStoreTest extends FunSpec with MustMatchers {
  val effectiveTimestamp = new DateTime(2015, 1, 15, 23, 43, 53, DateTimeZone.UTC)

  val eventStore = new SQLEventStore(new ConnectionProvider {
    override def getConnection(): Connection = ???
  }, now = () => effectiveTimestamp)

  it("can save events") {
    inTransaction { eventStore =>
      eventStore.save(serialized(ExampleEvent(21), ExampleEvent(22)))

      val all = eventStore.fromAll().events.toList

      all.map(_.effectiveTimestamp) must be(List(effectiveTimestamp, effectiveTimestamp))
      all.map(_.eventData).map(deserialize) must be(List(ExampleEvent(21),ExampleEvent(22)))
    }
  }

  it("can replay events from a given version number onwards") {
    inTransaction { eventStore =>
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))

      val previousVersion = eventStore.fromAll().events.toList.last.version

      eventStore.save(serialized(ExampleEvent(3), ExampleEvent(4)))

      val nextEvents = eventStore.fromAll(version = previousVersion).events.toList.map(_.eventData).map(deserialize)

      nextEvents must be(List(ExampleEvent(3), ExampleEvent(4)))
    }
  }

  it("returns no events if there are none past the specified version") {
    inTransaction { eventStore =>
      eventStore.fromAll(version = 900000).isEmpty must be(true)
    }
  }

  it("labels the last event of the stream") {
    inTransaction { eventStore =>
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))

      val nextEvents = eventStore.fromAll().events.toList

      nextEvents.map(_.last) must be(List(false, true))
    }
  }

  it("writes events to the current version of the stream when no expected version is specified") {
    inTransaction { eventStore =>
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))
      eventStore.save(serialized(ExampleEvent(3)))

      eventStore.fromAll(version = 2).events.toList.map(_.eventData).map(deserialize) must be(List(ExampleEvent(3)))
    }
  }

  it("throws OptimisticConcurrencyFailure when stream has already moved beyond the expected version") {
    inTransaction { eventStore =>
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))
      intercept[OptimisticConcurrencyFailure] {
        eventStore.save(serialized(ExampleEvent(3)), expectedVersion = Some(1))
      }
    }
  }

  it("throws OptimisticConcurrencyFailure when stream is not yet at the expected version") {
    inTransaction { eventStore =>
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))
      intercept[OptimisticConcurrencyFailure] {
        eventStore.save(serialized(ExampleEvent(3)), expectedVersion = Some(10))
      }
    }
  }

  it("throws OptimisticConcurrencyFailure when stream moves past expected version during save") {
    try {
      inTransaction { eventStore =>
        eventStore.fromAll()

        unrelatedSavesOfEventHappens()
        intercept[OptimisticConcurrencyFailure] {
          eventStore.save(serialized(ExampleEvent(3)), expectedVersion = Some(0))
        }
      }
    } finally {
      cleanup()
    }
  }

  it("returns nothing if eventstore is empty") {
    inTransaction { eventStore =>
      eventStore.fromAll().eventData.toList must be(Nil)
    }
  }


  it("returns only number of events asked for in batchSize") {
    inTransaction { eventStore =>
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2), ExampleEvent(3), ExampleEvent(4)))

      eventStore.fromAll(0, Some(2)).events.toList.map(_.eventData).map(deserialize) must be(List(ExampleEvent(1), ExampleEvent(2)))
      eventStore.fromAll(2, Some(2)).events.toList.map(_.eventData).map(deserialize) must be(List(ExampleEvent(3), ExampleEvent(4)))
    }
  }

  def cleanup(): Unit = {
    val conn = connect()
    conn.prepareStatement("delete from Event").execute()
    conn.close()
  }

  def unrelatedSavesOfEventHappens(): Unit = {
    new SQLEventStore(new ConnectionProvider {
      override def getConnection(): Connection = connect()
    }).save(serialized(ExampleEvent(3)))
  }

  case class ExampleEvent(a: Int)

  def serialized(evts: ExampleEvent*) = evts.map(serialize)

  def serialize(evt: ExampleEvent) = EventData(evt.getClass.getSimpleName, evt.a.toString.getBytes("UTF-8"))

  def deserialize(evt: EventData) = ExampleEvent(new String(evt.body, "UTF-8").toInt)

  def inTransaction[T](f: EventStore => T): T = {
    val connection: Connection = connect()

    try {
      connection.setAutoCommit(false)
      f(new SQLEventStore(SingleTransactionProvider(connection), now = () => effectiveTimestamp))
    } finally {
      connection.rollback()
    }
  }

  case class SingleTransactionProvider(connection: Connection) extends ConnectionProvider {
    override def getConnection() = NonClosingConnection(connection)
  }

  object NonClosingConnection {
    def apply(delegate: Connection) = newProxyInstance(this.getClass.getClassLoader, Array(classOf[Connection]),
      new InvocationHandler {
      override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
        if (method.getName == "close") {
          Unit
        } else {
          method.invoke(delegate, args :_*)
        }
      }
    }).asInstanceOf[Connection]
  }

  def connect() = {
    DriverManager.registerDriver(new com.mysql.jdbc.Driver())
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC")
  }
}