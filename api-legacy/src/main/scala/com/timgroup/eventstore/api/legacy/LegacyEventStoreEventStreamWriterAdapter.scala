package com.timgroup.eventstore.api.legacy

import com.timgroup.eventstore.api._
import javax.annotation.ParametersAreNonnullByDefault

import scala.collection.JavaConverters._

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
class LegacyEventStoreEventStreamWriterAdapter(eventstore: EventStore, pretendStreamId: StreamId) extends EventStreamWriter {

  def this(eventstore: EventStore) { this(eventstore, StreamId.streamId("all", "all")) }

  @ParametersAreNonnullByDefault
  override def write(streamId: StreamId, events: java.util.Collection[NewEvent]): Unit = {
    if (streamId != pretendStreamId) {
      throw new IllegalArgumentException("Attempting to save to stream " + streamId + " with legacy adapter.")
    }
    eventstore.save(events.asScala.map(toEventData).toSeq)
  }

  @ParametersAreNonnullByDefault
  override def write(streamId: StreamId, events: java.util.Collection[NewEvent], expectedVersion: Long): Unit = {
    if (streamId != pretendStreamId) {
      throw new IllegalArgumentException("Attempting to save to stream " + streamId + " with legacy adapter.")
    }
    eventstore.save(events.asScala.map(toEventData).toSeq, Some(expectedVersion))
  }

  override def toString = s"LegacyEventStoreEventStreamWriterAdapter{eventstore=$eventstore,previousStreamId=$pretendStreamId}"

  private def toEventData(newEvent: NewEvent): EventData = {
    if (newEvent.metadata().length != 0) {
      throw new IllegalArgumentException("Attempting to store metadata with legacy adapter.")
    }
    EventData(newEvent.`type`(), Body(newEvent.data()))
  }
}
