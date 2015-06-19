package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.EventInStream

trait EventHandler[T] {
  def apply(event: EventInStream, deserialized: T): Unit
}

class BroadcastingEventHandler[T](handlers: List[EventHandler[T]]) extends EventHandler[T] {
  override def apply(event: EventInStream, deserialized: T): Unit = handlers.foreach(_.apply(event, deserialized))
}