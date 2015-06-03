package com.timgroup.eventsubscription

import java.util.concurrent.Semaphore

import com.timgroup.eventstore.api._
import com.timgroup.eventstore.memory.InMemoryEventStore
import com.timgroup.eventsubscription.EventSubscriptionManager.SubscriptionSetup
import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Report
import com.timgroup.tucker.info.Status.{OK, WARNING}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone._
import org.mockito.Mockito._
import org.mockito.{Matchers, Mockito}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

import scala.util.Random

class EndToEndTest extends FunSpec with MustMatchers with BeforeAndAfterEach {
  var setup: SubscriptionSetup = _

  it("reports ill until caught up") {
    val store = new InMemoryEventStore()
    val eventProcessing = new BlockingEventHandler

    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = EventSubscriptionManager("test", store, List(eventProcessing), frequency = 1)
    setup.subscriptionManager.start()

    setup.health.get() must be(ill)

    eventProcessing.allowProcessing(3)

    eventually {
      setup.health.get() must be(healthy)
    }
  }

  it("reports current version") {
    val store = new InMemoryEventStore()
    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = EventSubscriptionManager("test", store, Nil)
    setup.subscriptionManager.start()
    val component = setup.components.find(_.getId == "event-stream-version-test").get

    eventually {
      component.getReport.getValue must be(3)
    }
  }

  it("reports warning if event store was not polled recently") {
    val initialTime = new DateTime(2015, 2, 20, 15, 21, 50, UTC)
    val clock = mock(classOf[Clock])
    when(clock.now()).thenReturn(initialTime)

    val eventStore = new HangingEventStore()

    eventStore.save(List(anEvent()))

    setup = EventSubscriptionManager("test", eventStore, Nil, clock)
    setup.subscriptionManager.start()

    eventually { setup.health.get() must be(healthy) }

    val component = setup.components.find(_.getId == "event-store-chaser-test").get

    component.getReport.getStatus must be(OK)

    eventStore.hangs()
    when(clock.now()).thenReturn(initialTime.plusSeconds(6))
    eventually { component.getReport.getStatus must be(WARNING) }
  }

  class HangingEventStore extends EventStore {
    private val underlying = new InMemoryEventStore()
    private var hanging = false

    def hangs() = hanging = true

    override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = underlying.save(newEvents, expectedVersion)

    override def fromAll(version: Long): EventStream = underlying.fromAll(version)

    override def fromAll(version: Long, eventHandler: (EventInStream) => Unit): Unit = {
      if (hanging) {
        Thread.sleep(10000)
      }
      underlying.fromAll(version, eventHandler)
    }
  }

  it("reports failure when event subscription terminates due to an eventhandler failure") {
    val store = new InMemoryEventStore()
    val failingHandler = mock(classOf[EventHandler])

    doThrow(new RuntimeException("failure")).when(failingHandler).apply(Matchers.any())

    store.save(List(anEvent()))

    setup = EventSubscriptionManager("test", store, List(failingHandler))
    setup.subscriptionManager.start()

    val component = setup.components.find(_.getId == "event-subscription-status-test").get

    eventually {
      component.getReport must be(new Report(WARNING, "Event subscription terminated: failure"))
    }
  }

  it("does not continue processing events if event processing failed on a previous event") {
    val now = DateTime.now()
    val store = new InMemoryEventStore(() => now)
    val failingHandler = mock(classOf[EventHandler])

    val evt1 = anEvent()
    val evt2 = anEvent()

    doThrow(new RuntimeException("failure")).when(failingHandler).apply(EventInStream(now, evt1, 1))

    store.save(List(evt1, evt2))

    setup = EventSubscriptionManager("test", store, List(failingHandler), frequency = 5)
    setup.subscriptionManager.start()

    val component = setup.components.find(_.getId == "event-subscription-status-test").get

    Thread.sleep(50)

    verify(failingHandler).apply(EventInStream(now, evt1, 1))
    verifyNoMoreInteractions(failingHandler)
    component.getReport must be(new Report(WARNING, "Event subscription terminated: failure"))

    store.save(List(anEvent()))

    Thread.sleep(50)

    component.getReport must be(new Report(WARNING, "Event subscription terminated: failure"))
  }


  override protected def afterEach(): Unit = {
    setup.subscriptionManager.stop()
  }

  def anEvent() = EventData("A", Body(Random.alphanumeric.take(10).mkString.getBytes("utf-8")))
}

class BlockingEventHandler extends EventHandler {
  val lock = new Semaphore(0)

  override def apply(event: EventInStream): Unit = {
    lock.acquire()
  }

  def allowProcessing(count: Int) {
    lock.release(count)
  }
}