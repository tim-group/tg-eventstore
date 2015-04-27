package com.timgroup.eventstore.memory

import com.timgroup.eventstore.api.{EventInStream, EventData, EventStream, EventStore}

class BackfillStitchingEventStore(backfill: EventStore, live: EventStore, liveCuttoffVersion: Long) extends EventStore {
  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = live.save(newEvents, expectedVersion)

  override def fromAll(version: Long): EventStream = {
    new EventStream {
      val events = backfill.fromAll(version) ++ live.fromAll(liveCuttoffVersion.max(version))

      override def next(): EventInStream = events.next()

      override def hasNext: Boolean = events.hasNext
    }
  }
}
