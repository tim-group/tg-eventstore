package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.EventSubscriptionListener
import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.{Component, Report, Status}
import org.joda.time.{DateTime, Seconds}
import org.slf4j.LoggerFactory

class EventStoreSubscriptionHealth(name: String, clock: Clock) extends Component(s"event-subscription-health-$name", s"Event subscription health ($name)") with EventSubscriptionListener {
  @volatile private var lastPollTimestamp: Option[DateTime] = None

  @volatile private var catastrophicFailure: Option[Exception] = None

  override def getReport = {
    if (catastrophicFailure.isDefined) {
      LoggerFactory.getLogger(getClass).warn("Event subscription terminated", catastrophicFailure.get)
      new Report(Status.WARNING, "Event subscription terminated: " + catastrophicFailure.get.getMessage)
    } else {
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
  }

  override def pollSucceeded(details: EventStorePolled): Unit = {
    lastPollTimestamp = Some(clock.now())
  }

  override def pollFailed(e: Exception): Unit = {}

  override def eventHandlerFailure(e: Exception): Unit = {
    catastrophicFailure = Some(e)
  }

  override def cycleCompleted(): Unit = {}
}
