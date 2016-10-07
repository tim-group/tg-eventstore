package com.timgroup.eventsubscription

import java.util.concurrent.Semaphore
import java.util.stream.Stream

import com.timgroup.eventstore.api._
import com.timgroup.eventstore.memory.InMemoryEventStore
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Report
import com.timgroup.tucker.info.Status.{CRITICAL, OK, WARNING}
import org.joda.time.DateTimeZone.UTC
import org.joda.time.{DateTime, DateTimeZone}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}
import scala.collection.JavaConversions._

import scala.util.Random

class EndToEndTest extends FunSpec with MustMatchers with BeforeAndAfterEach {
  var setup: EventSubscription[Event] = _

  it("reports ill during initial replay") {
    val clock = mock(classOf[Clock])
    val startTimestamp = new DateTime()
    when(clock.now()).thenReturn(startTimestamp)
    val store = new InMemoryEventStore()
    val eventProcessing = new BlockingEventHandler

    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = new EventSubscription("test", store, deserializer, List(eventProcessing), clock, 1024, 1L, 0L, 320, Nil)
    setup.start()
    val component = setup.statusComponents.find(_.getId == "event-subscription-status-test").get

    eventually {
      setup.health.get() must be(ill)
      component.getReport must be(new Report(OK, "Stale, catching up. No events processed yet. (Stale for 0s)"))
    }

    eventProcessing.allowProcessing(1)
    eventually {
      component.getReport must be(new Report(OK, "Stale, catching up. Currently at version 1. (Stale for 0s)"))
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

    setup = new EventSubscription("test", eventStore, deserializer, Nil, clock, 1024, 1L, 0L, 320, Nil)
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

    setup = new EventSubscription("test", store, deserializer, List(failingHandler), SystemClock, 1024, 1L, 0L, 320, Nil)
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

    doThrow(new RuntimeException("failure")).when(failingHandler).apply(Matchers.eq(EventInStream(timestamp, evt1, 1)), Matchers.eq(DeserializedVersionOf(EventInStream(timestamp, evt1, 1))), Matchers.anyBoolean())

    store.save(List(evt1, evt2))

    setup = new EventSubscription("test", store, deserializer, List(failingHandler), SystemClock, 1024, 5L, 0L, 320, Nil)
    setup.start()

    val component = setup.statusComponents.find(_.getId == "event-subscription-status-test").get

    Thread.sleep(50)

    verify(failingHandler).apply(Matchers.eq(EventInStream(timestamp, evt1, 1)), Matchers.eq(DeserializedVersionOf(EventInStream(timestamp, evt1, 1))), Matchers.anyBoolean())
    verifyNoMoreInteractions(failingHandler)
    component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")

    store.save(List(anEvent()))

    Thread.sleep(50)

    component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")
  }

  it("does not continue processing events if deserialization failed on a previous event") {
    val timestamp = DateTime.now()
    val store = new InMemoryEventStore(new Clock { def now() = timestamp })
    val handler = mock(classOf[EventHandler[Event]])

    val evt1 = anEvent()
    val evt2 = anEvent()
    val evt3 = anEvent()

    store.save(List(evt1, evt2, evt3))

    val failingDeserializer = new Deserializer[Event] {
      override def deserialize(evt: EventInStream): Event = {
        if (evt.version == 1) {
          throw new scala.RuntimeException("Failed to deserialize event 1")
        } else {
          deserializer.deserialize(evt)
        }
      }
    }

    setup = new EventSubscription("test", store, failingDeserializer, List(handler), SystemClock, 1024, 5L, 0L, 320, Nil)
    setup.start()

    Thread.sleep(50)

    verifyNoMoreInteractions(handler)
  }

  it("invokes event handlers with both EventInStream and deserialized event") {
    val timestamp = DateTime.now()

    val store = new InMemoryEventStore(new Clock { def now() = timestamp })

    val event1 = anEvent()
    val event2 = anEvent()

    store.save(List(event1, event2))

    val eventHandler = mock(classOf[EventHandler[Event]])
    setup = new EventSubscription("test", store, deserializer, List(eventHandler), SystemClock, 1024, 1L, 0L, 320, Nil)
    setup.start()

    eventually {
      setup.health.get() must be(healthy)
    }
 
    verify(eventHandler).apply(Matchers.eq(EventInStream(timestamp, event1, 1)), Matchers.eq(DeserializedVersionOf(EventInStream(timestamp, event1, 1))), Matchers.anyBoolean())
    verify(eventHandler).apply(Matchers.eq(EventInStream(timestamp, event2, 2)), Matchers.eq(DeserializedVersionOf(EventInStream(timestamp, event2, 2))), Matchers.eq(true))
  }

  it("starts up healthy when there are no events") {
    val store = new InMemoryEventStore()

    setup = new EventSubscription("test", store, deserializer, List(), SystemClock, 1024, 1L, 0L, 320, Nil)
    setup.start()

    eventually {
      setup.health.get() must be(healthy)
    }
  }

  it("starts up healthy when there are no events after fromVersion") {
    val store = new InMemoryEventStore()

    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = new EventSubscription("test", store, deserializer, List(), SystemClock, 1024, 1L, 3L, 320, Nil)
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

    override def streamingFromAll(version: Long): Stream[EventInStream] = {
      if (hanging) {
        Thread.sleep(10000)
      }
      underlying.streamingFromAll(version)
    }
  }

  override protected def afterEach(): Unit = {
    setup.stop()
  }

  val deserializer = new Deserializer[Event] {
    override def deserialize(event: EventInStream): Event = DeserializedVersionOf(event)
  }

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
