package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.eventsubscription.{ChaserListener, EventProcessorListener}
import com.timgroup.tucker.info.Health.State
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Status.{OK, WARNING}
import com.timgroup.tucker.info.{Component, Health, Report, Status}
import org.joda.time.DateTime
import org.joda.time.Seconds.secondsBetween
import org.slf4j.LoggerFactory

class EventSubscriptionStatus(name: String, clock: Clock = SystemClock) extends Component("event-subscription-status-" + name, "Event subscription status (" + name + ")") with Health with ChaserListener with EventProcessorListener {
  private val staleReport = new Report(WARNING, "Stale, catching up.")

  private val startTime: DateTime = clock.now()

  @volatile private var initialReplayDuration: Option[Int] = None
  @volatile private var liveVersion: Option[Long] = None
  @volatile private var processorVersion: Option[Long] = None
  @volatile private var subscriptionTerminated: Option[Exception] = None

  override def getReport: Report = {
    val upToDateAtVersion = for {
      endOfStream <- liveVersion
      lastProcessed <- processorVersion if endOfStream <= lastProcessed
    } yield {
      lastProcessed
    }

    if (subscriptionTerminated.isDefined) {
      new Report(Status.WARNING, "Event subscription terminated: " + subscriptionTerminated.get.getMessage)
    } else if (upToDateAtVersion.isEmpty) {
      staleReport
    } else {
      if (initialReplayDuration.get < 240) {
        new Report(OK, "Caught up. Initial replay took " + initialReplayDuration.get + "s")
      } else {
        new Report(WARNING, "Caught up. Initial replay took " + initialReplayDuration.get + "s. This is longer than expected limit of 240s.")
      }
    }
  }

  override def get(): State = if (initialReplayDuration.isDefined) healthy else ill

  override def chaserReceived(version: Long): Unit = {
    liveVersion = None
  }

  override def chaserUpToDate(version: Long): Unit = {
    liveVersion = Some(version)
    checkCaughtUp()
  }

  override def eventProcessed(version: Long): Unit = {
    processorVersion = Some(version)
    checkCaughtUp()
  }

  private def checkCaughtUp(): Unit = {
    for {
      endOfStream <- liveVersion
      lastProcessed <- processorVersion
    } yield {
      if (endOfStream <= lastProcessed) {
        if (initialReplayDuration.isEmpty) {
          initialReplayDuration = Some(secondsBetween(startTime, clock.now()).getSeconds)
        }
      }
    }
  }

  override def eventProcessingFailed(e: Exception): Unit = {
    LoggerFactory.getLogger(getClass).warn("Event subscription terminated", e)
    subscriptionTerminated = Some(e)
  }

  override def transientFailure(e: Exception): Unit = {}
}
