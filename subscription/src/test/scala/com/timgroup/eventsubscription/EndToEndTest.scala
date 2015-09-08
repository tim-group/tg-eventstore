package com.timgroup.eventsubscription

import java.util.concurrent.Semaphore

import com.timgroup.eventstore.api._
import com.timgroup.eventstore.memory.InMemoryEventStore
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Report
import com.timgroup.tucker.info.Status.{CRITICAL, OK, WARNING}
import org.joda.time.DateTimeZone.UTC
import org.joda.time.{DateTimeZone, DateTime}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

import scala.util.Random

class EndToEndTest extends FunSpec with MustMatchers with BeforeAndAfterEach {
  var setup: EventSubscription[Event] = _

  it("reports ill and warning on status page during initial replay") {
    val clock = mock(classOf[Clock])
    val startTimestamp = new DateTime()
    when(clock.now()).thenReturn(startTimestamp)
    val store = new InMemoryEventStore()
    val eventProcessing = new BlockingEventHandler

    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = new EventSubscription("test", store, deserializer, List(eventProcessing), runFrequency = 1, clock = clock)
    setup.start()
    val component = setup.statusComponents.find(_.getId == "event-subscription-status-test").get

    eventually {
      setup.health.get() must be(ill)
      component.getReport must be(new Report(WARNING, "Stale, catching up. No events processed yet. (Stale for 0s)"))
    }

    eventProcessing.allowProcessing(1)
    eventually {
      component.getReport must be(new Report(WARNING, "Stale, catching up. Currently at version 1. (Stale for 0s)"))
    }

    when(clock.now()).thenReturn(startTimestamp.plusSeconds(123))
    eventProcessing.allowProcessing(2)

    eventually {
      setup.health.get() must be(healthy)
      component.getReport must be(new Report(OK, "Caught up at version 3. Initial replay took 123s."))
    }
  }

  it("reports warning if event store was not polled recently") {
    val initialTime = new DateTime(2015, 2, 20, 15, 21, 50, UTC)
    val clock = mock(classOf[Clock])
    when(clock.now()).thenReturn(initialTime)

    val eventStore = new HangingEventStore()

    eventStore.save(List(anEvent()))

    setup = new EventSubscription("test", eventStore, deserializer, Nil, clock)
    setup.start()

    eventually { setup.health.get() must be(healthy) }

    val component = setup.statusComponents.find(_.getId == "event-store-chaser-test").get

    component.getReport.getStatus must be(OK)

    eventStore.hangs()
    when(clock.now()).thenReturn(initialTime.plusSeconds(6))
    eventually { component.getReport.getStatus must be(WARNING) }
  }

  it("reports failure when event subscription terminates due to an eventhandler failure") {
    val store = new InMemoryEventStore()
    val failingHandler = mock(classOf[EventHandler[Event]])

    doThrow(new RuntimeException("failure")).when(failingHandler).apply(Matchers.any(), Matchers.any(), Matchers.any())

    store.save(List(anEvent()))

    setup = new EventSubscription("test", store, deserializer, List(failingHandler))
    setup.start()

    val component = setup.statusComponents.find(_.getId == "event-subscription-status-test").get

    eventually {
      component.getReport.getStatus must be(CRITICAL)
      component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")
    }
  }

  it("does not continue processing events if event processing failed on a previous event") {
    val timestamp = DateTime.now()
    val store = new InMemoryEventStore(new Clock { def now() = timestamp })
    val failingHandler = mock(classOf[EventHandler[Event]])

    val evt1 = anEvent()
    val evt2 = anEvent()

    doThrow(new RuntimeException("failure")).when(failingHandler).apply(EventInStream(timestamp, evt1, 1), DeserializedVersionOf(EventInStream(timestamp, evt1, 1)), false)

    store.save(List(evt1, evt2))

    setup = new EventSubscription("test", store, deserializer, List(failingHandler), runFrequency = 5)
    setup.start()

    val component = setup.statusComponents.find(_.getId == "event-subscription-status-test").get

    Thread.sleep(50)

    verify(failingHandler).apply(EventInStream(timestamp, evt1, 1), DeserializedVersionOf(EventInStream(timestamp, evt1, 1)), false)
    verifyNoMoreInteractions(failingHandler)
    component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")

    store.save(List(anEvent()))

    Thread.sleep(50)

    component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")
  }

  it("invokes event handlers with both EventInStream and deserialized event") {
    val timestamp = DateTime.now()

    val store = new InMemoryEventStore(new Clock { def now() = timestamp })

    val event1 = anEvent()
    val event2 = anEvent()

    store.save(List(event1, event2))

    val eventHandler = mock(classOf[EventHandler[Event]])
    setup = new EventSubscription("test", store, deserializer, List(eventHandler), runFrequency = 1)
    setup.start()

    eventually {
      setup.health.get() must be(healthy)
    }

    verify(eventHandler).apply(EventInStream(timestamp, event1, 1), DeserializedVersionOf(EventInStream(timestamp, event1, 1)), false)
    verify(eventHandler).apply(EventInStream(timestamp, event2, 2), DeserializedVersionOf(EventInStream(timestamp, event2, 2)), true)
  }

  it("starts up healthy when there are no events") {
    val store = new InMemoryEventStore()

    setup = new EventSubscription("test", store, deserializer, List(), runFrequency = 1, fromVersion = 0)
    setup.start()

    eventually {
      setup.health.get() must be(healthy)
    }
  }

  it("starts up healthy when there are no events after fromVersion") {
    val store = new InMemoryEventStore()

    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = new EventSubscription("test", store, deserializer, List(), runFrequency = 1, fromVersion = 3)
    setup.start()

    eventually {
      setup.health.get() must be(healthy)
    }
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

  override protected def afterEach(): Unit = {
    setup.stop()
  }

  def deserializer(evt: EventInStream) = DeserializedVersionOf(evt)

  def anEvent() = EventData("A", Body(Random.alphanumeric.take(10).mkString.getBytes("utf-8")))
}

trait Event

case class DeserializedVersionOf(evt: EventInStream) extends Event

class BlockingEventHandler extends EventHandler[Event] {
  val lock = new Semaphore(0)

  override def apply(event: EventInStream, deserialized: Event, endOfBatch: Boolean): Unit = {
    lock.acquire()
  }

  def allowProcessing(count: Int) {
    lock.release(count)
  }
}
