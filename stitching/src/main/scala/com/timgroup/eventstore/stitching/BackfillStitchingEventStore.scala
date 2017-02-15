package com.timgroup.eventstore.stitching

import java.util.stream.Stream

import com.timgroup.eventstore.api.{EventData, EventInStream, EventStore, EventStream}

/**
  * @deprecated uaw BackfillStitchingEventSource instead
  */
@Deprecated
class BackfillStitchingEventStore(backfill: EventStore, live: EventStore, liveCuttoffVersion: Long) extends EventStore {
  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = live.save(newEvents, expectedVersion)

  override def fromAll(version: Long) = new EventStream {
    val events = Iterator.empty ++ // Note: Workaround for https://issues.scala-lang.org/browse/SI-9623
      backfill.fromAll(version) ++ live.fromAll(liveCuttoffVersion.max(version))

    override def next(): EventInStream = events.next()

    override def hasNext: Boolean = events.hasNext
  }

  override def streamingFromAll(version: Long): Stream[EventInStream] = if (version <= liveCuttoffVersion) java.util.stream.Stream.concat(backfill.streamingFromAll(version), live.streamingFromAll(liveCuttoffVersion.max(version))) else live.streamingFromAll(liveCuttoffVersion.max(version))
}
