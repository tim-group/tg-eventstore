package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.EventSubscriptionListener
import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.tucker.info.Health.State
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Status.{OK, WARNING}
import com.timgroup.tucker.info.{Component, Health, Report, Status}
import org.joda.time.DateTime
import org.joda.time.Seconds.secondsBetween
import org.slf4j.LoggerFactory

class EventSubscriptionStatus(name: String, clock: Clock = SystemClock) extends Component("event-subscription-status-" + name, "Event subscription status (" + name + ")") with Health with EventSubscriptionListener {
  @volatile private var currentHealth = ill
  @volatile private var currentState = new Report(WARNING, "Not running")
  @volatile private var startTime: DateTime = null
  @volatile private var finishTime: DateTime = null

  private def caughtUpReport = new Report(OK, "Caught up. Initial replay took " + secondsBetween(startTime, finishTime).getSeconds + "s")

  override def getReport: Report = currentState

  override def get(): State = currentHealth

  override def eventSubscriptionStarted(): Unit = {
    startTime = clock.now()
    currentState = new Report(WARNING, "Event subscription started. Catching up.")
  }

  override def initialReplayCompleted(): Unit = {
    finishTime = clock.now()
    currentHealth = healthy
    currentState = caughtUpReport
  }

  override def newEventsFound(): Unit = {
    currentState = new Report(WARNING, "Stale")
  }

  override def caughtUp(): Unit = {
    currentState = caughtUpReport
  }

  override def eventHandlerFailure(e: Exception): Unit = {
    LoggerFactory.getLogger(getClass).warn("Event subscription terminated", e)
    currentState = new Report(Status.WARNING, "Event subscription terminated: " + e.getMessage)
  }

  override def pollSucceeded(): Unit = {}

  override def cycleCompleted(): Unit = {}


  override def pollFailed(e: Exception): Unit = {}
}
