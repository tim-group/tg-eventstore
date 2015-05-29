package com.timgroup.eventstore.stitching

import com.timgroup.eventstore.api.{EventInStream, EventData}
import com.timgroup.eventstore.memory.InMemoryEventStore
import org.scalatest.{FunSpec, MustMatchers}

class BackfillStitchingEventStoreTest extends FunSpec with MustMatchers {
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
    eventStore.fromAll(0, evt => events = events :+ evt.eventData)

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
    eventStore.fromAll(5, evt => events = events :+ evt.eventData)

    events must be(List(
      event("L2")
    ))
  }

  def event(eventType: String) = EventData(eventType, eventType.getBytes("UTF-8"))
}
