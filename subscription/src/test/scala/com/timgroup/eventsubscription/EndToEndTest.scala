package com.timgroup.eventsubscription

import java.time.Clock.systemUTC
import java.time.{Clock, Instant}
import java.util.concurrent.Semaphore
import java.util.stream.Stream

import com.timgroup.eventstore.api.EventRecord.eventRecord
import com.timgroup.eventstore.api._
import com.timgroup.eventstore.memory.InMemoryEventStore
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Report
import com.timgroup.tucker.info.Status.{CRITICAL, OK, WARNING}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
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
    val startTimestamp = Instant.now()
    when(clock.instant()).thenReturn(startTimestamp)
    val store = new InMemoryEventStore()
    val eventProcessing = new BlockingEventHandler

    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(store), deserializer, List(eventProcessing), clock, 1024, 1L, LegacyPositionAdapter(0L), 320, Nil)
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

    when(clock.instant()).thenReturn(startTimestamp.plusSeconds(123))
    eventProcessing.allowProcessing(2)

    eventually {
      setup.health.get() must be(healthy)
      component.getReport must be(new Report(OK, "Caught up at version 3. Initial replay took 123s."))
    }
  }

  it("reports warning if event store was not polled recently") {
    val initialTime = Instant.parse("2015-02-20T15:21:50.000Z")
    val clock = mock(classOf[Clock])
    when(clock.instant()).thenReturn(initialTime)

    val eventStore = new HangingEventStore()

    eventStore.save(List(anEvent()))

    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(eventStore), deserializer, Nil, clock, 1024, 1L, LegacyPositionAdapter(0L), 320, Nil)
    setup.start()

    eventually { setup.health.get() must be(healthy) }

    val component = setup.statusComponents.find(_.getId == "event-store-chaser-test").get

    component.getReport.getStatus must be(OK)

    eventStore.hangs()
    when(clock.instant()).thenReturn(initialTime.plusSeconds(6))
    eventually { component.getReport.getStatus must be(WARNING) }
  }

  it("reports failure when event subscription terminates due to an eventhandler failure") {
    val store = new InMemoryEventStore()
    val failingHandler = mock(classOf[EventHandler[Event]])

    doThrow(new RuntimeException("failure")).when(failingHandler).apply(Matchers.any(), Matchers.any(), Matchers.any(), Matchers.any())

    store.save(List(anEvent()))

    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(store), deserializer, List(failingHandler), systemUTC(), 1024, 1L, LegacyPositionAdapter(0L), 320, Nil)
    setup.start()

    val component = setup.statusComponents.find(_.getId == "event-subscription-status-test").get

    eventually {
      component.getReport.getStatus must be(CRITICAL)
      component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")
    }
  }

  it("does not continue processing events if event processing failed on a previous event") {
    val timestamp = DateTime.now()
    val store = new InMemoryEventStore(new com.timgroup.eventstore.api.Clock { def now() = timestamp })
    val failingHandler = mock(classOf[EventHandler[Event]])

    val evt1 = anEvent()
    val evt2 = anEvent()

    doThrow(new RuntimeException("failure")).when(failingHandler)
      .apply(Matchers.eq(LegacyPositionAdapter(1)),
             Matchers.eq(timestamp),
             Matchers.eq(DeserializedVersionOf(eventRecord(Instant.ofEpochMilli(timestamp.getMillis), StreamId.streamId("all", "all"), 1, evt1.eventType, evt1.body.data, new Array(0)))),
             Matchers.anyBoolean())

    store.save(List(evt1, evt2))

    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(store), deserializer, List(failingHandler), systemUTC(), 1024, 5L, LegacyPositionAdapter(0L), 320, Nil)
    setup.start()

    val component = setup.statusComponents.find(_.getId == "event-subscription-status-test").get

    Thread.sleep(50)

    verify(failingHandler).apply(
      Matchers.eq(LegacyPositionAdapter(1)),
      Matchers.eq(timestamp),
      Matchers.eq(DeserializedVersionOf(eventRecord(Instant.ofEpochMilli(timestamp.getMillis), StreamId.streamId("all", "all"), 1, evt1.eventType, evt1.body.data, new Array(0)))),
      Matchers.anyBoolean())
    verifyNoMoreInteractions(failingHandler)
    component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")

    store.save(List(anEvent()))

    Thread.sleep(50)

    component.getReport.getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: failure")
  }

  it("does not continue processing events if deserialization failed on a previous event") {
    val timestamp = DateTime.now()
    val store = new InMemoryEventStore(new com.timgroup.eventstore.api.Clock { def now() = timestamp })
    val handler = mock(classOf[EventHandler[Event]])

    val evt1 = anEvent()
    val evt2 = anEvent()
    val evt3 = anEvent()

    store.save(List(evt1, evt2, evt3))

    val failingDeserializer = new Deserializer[Event] {
      override def deserialize(evt: EventRecord): Event = {
        if (evt.eventNumber() == 1) {
          throw new scala.RuntimeException("Failed to deserialize event 1")
        } else {
          deserializer.deserialize(evt)
        }
      }
    }

    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(store), failingDeserializer, List(handler), systemUTC(), 1024, 5L, LegacyPositionAdapter(0L), 320, Nil)
    setup.start()

    Thread.sleep(50)

    verifyNoMoreInteractions(handler)
  }

  it("invokes event handlers with both EventInStream and deserialized event") {
    val timestamp = DateTime.now()

    val store = new InMemoryEventStore(new com.timgroup.eventstore.api.Clock { def now() = timestamp })

    val event1 = anEvent()
    val event2 = anEvent()

    store.save(List(event1, event2))

    val eventHandler = mock(classOf[EventHandler[Event]])
    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(store), deserializer, List(eventHandler), systemUTC(), 1024, 1L, LegacyPositionAdapter(0L), 320, Nil)
    setup.start()

    eventually {
      setup.health.get() must be(healthy)
    }
 
    verify(eventHandler).apply(
      Matchers.eq(LegacyPositionAdapter(1)),
      Matchers.eq(timestamp),
      Matchers.eq(DeserializedVersionOf(eventRecord(Instant.ofEpochMilli(timestamp.getMillis), StreamId.streamId("all", "all"), 1, event1.eventType, event1.body.data, new Array(0)))),
      Matchers.anyBoolean())

    verify(eventHandler).apply(
      Matchers.eq(LegacyPositionAdapter(2)),
      Matchers.eq(timestamp),
      Matchers.eq(DeserializedVersionOf(eventRecord(Instant.ofEpochMilli(timestamp.getMillis), StreamId.streamId("all", "all"), 2, event2.eventType, event2.body.data, new Array(0)))),
      Matchers.eq(true))
  }

  it("starts up healthy when there are no events") {
    val store = new InMemoryEventStore()

    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(store), deserializer, List(), systemUTC(), 1024, 1L, LegacyPositionAdapter(0L), 320, Nil)
    setup.start()

    eventually {
      setup.health.get() must be(healthy)
    }
  }

  it("starts up healthy when there are no events after fromVersion") {
    val store = new InMemoryEventStore()

    store.save(List(anEvent(), anEvent(), anEvent()))

    setup = new EventSubscription("test", new LegacyEventStoreEventReaderAdapter(store), deserializer, List(), systemUTC(), 1024, 1L, LegacyPositionAdapter(3L), 320, Nil)
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
    override def deserialize(event: EventRecord): Event = DeserializedVersionOf(event)
  }

  def anEvent() = EventData("A", Body(Random.alphanumeric.take(10).mkString.getBytes("utf-8")))
}

trait Event

case class DeserializedVersionOf(evt: EventRecord) extends Event

class BlockingEventHandler extends EventHandler[Event] {
  val lock = new Semaphore(0)

  override def apply(position: Position, timestamp: DateTime, deserialized: Event, endOfBatch: Boolean): Unit = {
    lock.acquire()
  }

  def allowProcessing(count: Int) {
    lock.release(count)
  }
}
