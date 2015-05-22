package com.timgroup.eventsubscription

import java.sql.{DriverManager, Connection}
import java.util.concurrent.{Callable, Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicInteger

import com.timgroup.eventstore.api.{EventData, EventInStream, EventStoreTest}
import com.timgroup.eventstore.mysql.legacy.AutoIncrementBasedEventPersister
import com.timgroup.eventstore.mysql.{EventAtATime, SQLEventStore, ConnectionProvider}
import com.timgroup.eventsubscription.EventSubscriptionManager.SubscriptionSetup
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, MustMatchers, FunSpec}

class EventSkipFailureTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
  var setup: SubscriptionSetup = _


  private val connectionProvider = new ConnectionProvider {
    override def getConnection(): Connection = {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver())
      DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC")
    }
  }

  override protected def beforeEach(): Unit = {
    val conn = connectionProvider.getConnection()
    conn.prepareStatement("DROP TABLE IF EXISTS EventLive").execute()
    conn.prepareStatement("CREATE TABLE EventLive(eventType VARCHAR(255), body BLOB, version INT PRIMARY KEY AUTO_INCREMENT, effective_timestamp datetime)").execute()
    conn.close()
  }

  ignore("does not skip events when transactions commit out of order") {
    val count = new AtomicInteger(0)

    val store = SQLEventStore(connectionProvider, "EventLive")
    setup = EventSubscriptionManager("test", store, List(new EventHandler {
      override def apply(event: EventInStream): Unit = count.incrementAndGet()
    }), frequency = 1)

    setup.subscriptionManager.start()

    val executor = Executors.newFixedThreadPool(2)

    val futures = (1 to 100).map(i => executor.submit(new Callable[Unit] {
      override def call(): Unit = {
        val conn = connectionProvider.getConnection()
        try {
          conn.setAutoCommit(false)
          new AutoIncrementBasedEventPersister("EventLive").saveEventsToDB(conn, Seq(EventAtATime(new DateTime(), EventData("Blah", "".getBytes("utf-8")))))

          if (i % 2 == 0) {
            Thread.sleep(100)
          }

          conn.commit()
        } finally {
          conn.close()
        }
      }
    }))


    futures.foreach(_.get())

    Thread.sleep(2000)

    count.get() must be(100)
  }
}
