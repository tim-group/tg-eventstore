package com.timgroup.mysqleventstore

import java.lang.reflect.Proxy.newProxyInstance
import java.lang.reflect.{Method, InvocationHandler}
import java.sql.{Connection, DriverManager}

import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class SQLEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
  val connection = connect()

  val eventStore = new SQLEventStore(SingleTransactionProvider(connection), now = () => effectiveTimestamp)

  it should behave like anEventStore(eventStore)

  it("throws OptimisticConcurrencyFailure when stream moves past expected version during save") {
    eventStore.fromAll()

    unrelatedSavesOfEventHappens()
    intercept[OptimisticConcurrencyFailure] {
      eventStore.save(serialized(ExampleEvent(3)), expectedVersion = Some(0))
    }
  }

  override protected def beforeEach(): Unit = {
    connection.setAutoCommit(false)
  }

  override protected def afterEach(): Unit = {
    connection.rollback()
    val conn = connect()
    conn.prepareStatement("delete from Event").execute()
    conn.close()
  }

  def unrelatedSavesOfEventHappens(): Unit = {
    new SQLEventStore(new ConnectionProvider {
      override def getConnection(): Connection = connect()
    }).save(serialized(ExampleEvent(3)))
  }

  case class SingleTransactionProvider(connection: Connection) extends ConnectionProvider {
    override def getConnection() = NonClosingConnection(connection)
  }

  object NonClosingConnection {
    def apply(delegate: Connection) = newProxyInstance(this.getClass.getClassLoader, Array(classOf[Connection]),
      new InvocationHandler {
      override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
        if (List("close", "setAutoCommit", "commit", "rollback").contains(method.getName)) {
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