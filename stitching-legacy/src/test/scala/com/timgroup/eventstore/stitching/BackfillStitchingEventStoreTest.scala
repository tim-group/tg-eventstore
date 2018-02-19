package com.timgroup.eventstore.stitching

import java.util.function.Consumer

import com.timgroup.eventstore.api._
import com.timgroup.eventstore.memory.InMemoryEventStore
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class BackfillStitchingEventStoreTest extends FunSpec with MustMatchers with EventStoreTest with BeforeAndAfterEach {
  it("reads all events from backfill, and those required from live when querying entire eventstream") {
    val backfill = new InMemoryEventStore()
    val live = new InMemoryEventStore()

    backfill.save(Seq(event("B1"), event("B2"), event("B3")))
    live.save(Seq(
      event("old-1"), event("old-2"), event("old-3"), event("old-4"),
      event("L1"), event("L2")))

    val eventStore = new BackfillStitchingEventStore(backfill, live, 4)

    eventStore.fromAll().toList.map(_.eventData) must be(List(
      event("B1"),
      event("B2"),
      event("B3"),
      event("L1"),
      event("L2")
    ))

    var events = Vector[EventData]()
    eventStore.streamingFromAll(0).forEachOrdered(new Consumer[EventInStream] {
      override def accept(evt: EventInStream): Unit = events = events :+ evt.eventData
    })

    events must be(List(
      event("B1"),
      event("B2"),
      event("B3"),
      event("L1"),
      event("L2")
    ))
  }

  it("returns only events from live if cutoff is before fromVersion") {
    val backfill = new InMemoryEventStore()
    val live = new InMemoryEventStore()

    backfill.save(Seq(event("B1"), event("B2"), event("B3")))
    live.save(Seq(
      event("old-1"), event("old-2"), event("old-3"), event("old-4"),
      event("L1"), event("L2")))

    val eventStore = new BackfillStitchingEventStore(backfill, live, 4)

    eventStore.fromAll(5).toList.map(_.eventData) must be(List(
      event("L2")
    ))

    var events = Vector[EventData]()
    eventStore.streamingFromAll(5).forEachOrdered(new Consumer[EventInStream] {
      override def accept(evt: EventInStream): Unit = events = events :+ evt.eventData
    })

    events must be(List(
      event("L2")
    ))
  }

  it("does not interleave events if additional events are added to backfill") {
    val backfill = new InMemoryEventStore()
    val live = new InMemoryEventStore()

    backfill.save(Seq(event("B1")))
    live.save(Seq(event("L1"), event("L2")))

    val eventStore = new BackfillStitchingEventStore(backfill, live, 0)

    val events = eventStore.fromAll(0)

    events.hasNext mustBe true
    events.next.eventData mustBe event("B1")
    events.hasNext mustBe true
    events.next.eventData mustBe event("L1")

    // Add another event to backfill
    backfill.save(Seq(event("B2")))

    events.hasNext mustBe true
    events.next.eventData mustBe event("L2")
  }

  val live = new InMemoryEventStore(
    now = new Clock {
      override def now(): DateTime = effectiveTimestamp
    }
  )

  val eventStore = {
    val backfill = new InMemoryEventStore()
    new BackfillStitchingEventStore(backfill, live, 0)
  }

  it should behave like anEventStore(eventStore)

  it should behave like optimisticConcurrencyControl(eventStore)

  def event(eventType: String) = EventData(eventType, eventType.getBytes("UTF-8"))

  override protected def afterEach(): Unit = {
    live.clear()
  }
}
