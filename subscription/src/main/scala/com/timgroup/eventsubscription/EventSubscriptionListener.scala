package com.timgroup.eventsubscription

import com.timgroup.eventsubscription.healthcheck.EventStorePolled

trait EventSubscriptionListener {
  def pollSucceeded(): Unit

  def pollFailed(e: Exception): Unit

  def eventHandlerFailure(e: Exception): Unit

  def cycleCompleted(): Unit

  def initialReplayCompleted(): Unit

  def newEventsFound(): Unit

  def caughtUp(): Unit

  def eventSubscriptionStarted(): Unit
}

class BroadcastingListener(listeners: EventSubscriptionListener*) extends EventSubscriptionListener {
  override def pollSucceeded(): Unit = listeners.foreach(_.pollSucceeded())

  override def cycleCompleted(): Unit = listeners.foreach(_.cycleCompleted())

  override def eventHandlerFailure(e: Exception): Unit = listeners.foreach(_.eventHandlerFailure(e))

  override def pollFailed(e: Exception): Unit = listeners.foreach(_.pollFailed(e))

  override def initialReplayCompleted(): Unit = listeners.foreach(_.initialReplayCompleted())

  override def newEventsFound(): Unit = listeners.foreach(_.newEventsFound())

  override def caughtUp(): Unit = listeners.foreach(_.caughtUp())

  override def eventSubscriptionStarted(): Unit = listeners.foreach(_.eventSubscriptionStarted())
}