package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.EventPage

trait EventHandler {
  def apply(event: EventPage): Unit
}

class BroadcastingEventHandler(handlers: List[EventHandler]) extends EventHandler {
  override def apply(event: EventPage): Unit = handlers.foreach(_.apply(event))
}