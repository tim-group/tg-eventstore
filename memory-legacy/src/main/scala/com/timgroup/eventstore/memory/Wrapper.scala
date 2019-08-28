package com.timgroup.eventstore.memory

import java.util.function.{LongFunction, ToLongFunction}

import com.timgroup.eventstore.api.{EventStore, Position, StreamId}
import com.timgroup.eventstore.api.legacy.LegacyStore

object Wrapper {
  private val toPosition: LongFunction[Position] = new LongFunction[Position] {
    override def apply(value: Long): Position = JavaInMemoryEventStore.CODEC.deserializePosition(value.toString)
  }

  private val fromPosition: ToLongFunction[Position] = new ToLongFunction[Position] {
    override def applyAsLong(value: Position): Long = JavaInMemoryEventStore.CODEC.serializePosition(value).toLong
  }

  def toLegacy(underlying : JavaInMemoryEventStore): EventStore = new LegacyStore(underlying, underlying, StreamId.streamId("all", "all"), toPosition, fromPosition)

  implicit class Conversion(underlying : JavaInMemoryEventStore) {
    def toLegacy: EventStore = Wrapper.toLegacy(underlying)
  }
}
