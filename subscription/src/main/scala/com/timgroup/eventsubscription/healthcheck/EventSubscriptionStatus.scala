package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.EventSubscriptionListener
import com.timgroup.tucker.info.Health.State
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Status.{OK, WARNING}
import com.timgroup.tucker.info.{Component, Health, Report, Status}
import org.slf4j.LoggerFactory

class EventSubscriptionStatus(name: String) extends Component("event-subscription-status-" + name, "Event subscription status (" + name + ")") with Health with EventSubscriptionListener {
  @volatile private var currentHealth = ill
  @volatile private var currentState = new Report(WARNING, "Not running")

  override def getReport: Report = currentState

  override def get(): State = currentHealth

  override def eventSubscriptionStarted(): Unit = {
    currentState = new Report(WARNING, "Event subscription started")
  }

  override def initialReplayCompleted(): Unit = {
    currentHealth = healthy
    currentState = new Report(OK, "Caught up")
  }

  override def newEventsFound(): Unit = {
    currentState = new Report(WARNING, "Stale")
  }

  override def caughtUp(): Unit = {
    currentState = new Report(OK, "Caught up")
  }

  override def eventHandlerFailure(e: Exception): Unit = {
    LoggerFactory.getLogger(getClass).warn("Event subscription terminated", e)
    currentState = new Report(Status.WARNING, "Event subscription terminated: " + e.getMessage)
  }

  override def pollSucceeded(): Unit = {}

  override def cycleCompleted(): Unit = {}


  override def pollFailed(e: Exception): Unit = {}
}
