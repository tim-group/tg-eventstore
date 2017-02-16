package com.timgroup.eventstore.api.legacy

import com.timgroup.eventstore.api._

import scala.collection.JavaConversions._

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
class LegacyEventStoreEventStreamWriterAdapter(eventstore: EventStore, pretendStreamId: StreamId) extends EventStreamWriter {

  def this(eventstore: EventStore) { this(eventstore, StreamId.streamId("all", "all")) }

  override def write(streamId: StreamId, events: java.util.Collection[NewEvent]): Unit = {
    if (streamId != pretendStreamId) {
      throw new IllegalArgumentException("Attempting to save to stream " + streamId + " with legacy adapter.")
    }
    eventstore.save(events.toList.map(toEventData))
  }

  override def write(streamId: StreamId, events: java.util.Collection[NewEvent], expectedVersion: Long): Unit = {
    if (streamId != pretendStreamId) {
      throw new IllegalArgumentException("Attempting to save to stream " + streamId + " with legacy adapter.")
    }
    eventstore.save(events.toList.map(toEventData), Some(expectedVersion))
  }

  private def toEventData(newEvent: NewEvent): EventData = {
    if (newEvent.metadata().length != 0) {
      throw new IllegalArgumentException("Attempting to store metadata with legacy adapter.")
    }
    EventData(newEvent.`type`(), Body(newEvent.data()))
  }
}
