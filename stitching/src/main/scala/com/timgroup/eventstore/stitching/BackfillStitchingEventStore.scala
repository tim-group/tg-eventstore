package com.timgroup.eventstore.stitching

import com.timgroup.eventstore.api.{EventData, EventInStream, EventStore, EventStream}

class BackfillStitchingEventStore(backfill: EventStore, live: EventStore, liveCuttoffVersion: Long) extends EventStore {
  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = live.save(newEvents, expectedVersion)

  override def fromAll(version: Long): EventStream = {
    new EventStream {
      val events = backfill.fromAll(version) ++ live.fromAll(liveCuttoffVersion.max(version))

      override def next(): EventInStream = events.next()

      override def hasNext: Boolean = events.hasNext
    }
  }

  override def fromAll(version: Long, eventHandler: (EventInStream) => Unit): Unit = {
    if (version <= liveCuttoffVersion) {
      backfill.fromAll(version, eventHandler)
    }
    live.fromAll(liveCuttoffVersion.max(version), eventHandler)
  }
}
