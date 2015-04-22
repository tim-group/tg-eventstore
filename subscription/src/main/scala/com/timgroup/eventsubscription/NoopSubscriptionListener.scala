package com.timgroup.eventsubscription

import com.timgroup.eventsubscription.healthcheck.EventStorePolled

object NoopSubscriptionListener extends EventSubscriptionListener {
  override def pollSucceeded(details: EventStorePolled): Unit = {}

  override def pollFailed(e: Exception): Unit = {}

  override def eventHandlerFailure(e: Exception): Unit = {}

  override def cycleCompleted(): Unit = {}
}
