package com.timgroup.eventstore.api.legacy

import java.nio.charset.StandardCharsets.UTF_8
import java.util.function.{LongFunction, ToLongFunction}

import com.timgroup.eventstore.api.{EventData, EventReader, EventStreamWriter, NewEvent, OptimisticConcurrencyFailure, Position, StreamId, WrongExpectedVersionException}

import scala.collection.JavaConverters._

class LegacyStore(eventReader: EventReader, private val writer: EventStreamWriter, private val streamId: StreamId, toPosition: LongFunction[_ <: Position], toVersion: ToLongFunction[_ >: Position])
  extends ReadableLegacyStore(eventReader, toPosition, toVersion) {

  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = {
    expectedVersion match {
      case Some(v) =>
        try {
          writer.write(streamId, newEvents.map(LegacyStore.toNewEvent).asJavaCollection, v - 1)
        } catch {
          case e: WrongExpectedVersionException =>
            throw new OptimisticConcurrencyFailure(Some(e))
        }
      case None =>
        writer.write(streamId, newEvents.map(LegacyStore.toNewEvent).asJavaCollection)
    }
  }
}

object LegacyStore {
  private val DefaultMetadata = "{}".getBytes(UTF_8)
  def toNewEvent(e: EventData): NewEvent = NewEvent.newEvent(e.eventType, e.body.data, DefaultMetadata)
}
