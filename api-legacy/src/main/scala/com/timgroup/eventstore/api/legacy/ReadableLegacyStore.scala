package com.timgroup.eventstore.api.legacy

import java.util.function.{LongFunction, ToLongFunction}
import java.util.stream

import com.timgroup.eventstore.api.{EventData, EventInStream, EventReader, EventStore, EventStream, Position, ResolvedEvent}

class ReadableLegacyStore(private val eventReader: EventReader, private val toPosition: LongFunction[_ <: Position], private val toVersion: ToLongFunction[_ >: Position]) extends EventStore {
  override def fromAll(version: Long): EventStream = new EventStream {
    private val underlying = eventReader.readAllForwards(toPosition.apply(version)).iterator()

    override def hasNext: Boolean = underlying.hasNext

    override def next(): EventInStream = toEventInStream(underlying.next())
  }

  override def streamingFromAll(version: Long): stream.Stream[EventInStream] = eventReader.readAllForwards(toPosition.apply(version)).map(new java.util.function.Function[ResolvedEvent, EventInStream] {
    override def apply(t: ResolvedEvent): EventInStream = toEventInStream(t)
  })

  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = throw new UnsupportedOperationException()

  override def toString: String = eventReader.toString

  private def toEventInStream(in: ResolvedEvent) = EventInStream(
    new org.joda.time.DateTime(in.eventRecord().timestamp().toEpochMilli, org.joda.time.DateTimeZone.UTC),
    EventData(in.eventRecord().eventType(), in.eventRecord().data()),
    toVersion.applyAsLong(in.position())
  )
}
