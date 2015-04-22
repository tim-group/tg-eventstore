package com.timgroup.eventsubscription

import com.timgroup.eventsubscription.healthcheck.EventStorePolled

trait EventSubscriptionListener {
  def pollSucceeded(details: EventStorePolled): Unit

  def pollFailed(e: Exception): Unit

  def eventHandlerFailure(e: Exception): Unit

  def cycleCompleted(): Unit
}

class BroadcastingListener(listeners: EventSubscriptionListener*) extends EventSubscriptionListener {
  override def pollSucceeded(details: EventStorePolled): Unit = listeners.foreach(_.pollSucceeded(details))

  override def cycleCompleted(): Unit = listeners.foreach(_.cycleCompleted())

  override def eventHandlerFailure(e: Exception): Unit = listeners.foreach(_.eventHandlerFailure(e))

  override def pollFailed(e: Exception): Unit = listeners.foreach(_.pollFailed(e))
}