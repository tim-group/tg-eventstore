package com.timgroup.eventsubscription

import com.timgroup.eventsubscription.healthcheck.EventStorePolled

object NoopSubscriptionListener extends EventSubscriptionListener {
  override def pollSucceeded(): Unit = {}

  override def pollFailed(e: Exception): Unit = {}

  override def eventHandlerFailure(e: Exception): Unit = {}

  override def cycleCompleted(): Unit = {}

  override def initialReplayCompleted(): Unit = {}

  override def newEventsFound(): Unit = {}

  override def caughtUp(): Unit = {}

  override def eventSubscriptionStarted(): Unit = {}
}
