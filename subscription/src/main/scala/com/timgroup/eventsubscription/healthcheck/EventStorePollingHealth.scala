package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.EventSubscriptionListener
import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.{Component, Report, Status}
import org.joda.time.{DateTime, Seconds}
import org.slf4j.LoggerFactory

class EventStorePollingHealth(name: String, clock: Clock) extends Component("event-store-polling-" + name, "Event subscription health (" + name + ")") with EventSubscriptionListener {
  @volatile private var lastPollTimestamp: Option[DateTime] = None

  override def getReport = {
    lastPollTimestamp match {
      case None => new Report(Status.OK, "Did not yet poll eventstore")
      case Some(timestamp) => {
        val seconds = Seconds.secondsBetween(timestamp, clock.now()).getSeconds
        if (seconds > 5) {
          new Report(Status.WARNING, "Did not poll the eventstore since %s. (%s seconds ago)".format(timestamp, seconds))
        } else {
          new Report(Status.OK, "Polled eventstore at %s. (%s seconds ago)".format(timestamp, seconds))
        }
      }
    }
  }

  override def pollSucceeded(): Unit = {
    lastPollTimestamp = Some(clock.now())
  }

  override def pollFailed(e: Exception): Unit = {}

  override def eventHandlerFailure(e: Exception): Unit = {}

  override def cycleCompleted(): Unit = {}

  override def initialReplayCompleted(): Unit = {}

  override def newEventsFound(): Unit = {}

  override def caughtUp(): Unit = {}

  override def eventSubscriptionStarted(): Unit = {}
}
