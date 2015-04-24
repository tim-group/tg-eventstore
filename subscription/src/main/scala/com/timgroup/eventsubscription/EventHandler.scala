package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.EventInStream

trait EventHandler {
  def apply(event: EventInStream): Unit
}

class BroadcastingEventHandler(handlers: List[EventHandler]) extends EventHandler {
  override def apply(event: EventInStream): Unit = handlers.foreach(_.apply(event))
}