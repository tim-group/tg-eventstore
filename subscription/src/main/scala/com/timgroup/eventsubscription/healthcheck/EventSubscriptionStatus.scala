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
  private def caughtUpReport = new Report(OK, "Caught up. Initial replay took " + secondsBetween(startTime, finishTime).getSeconds + "s")
  private val staleReport = new Report(WARNING, "Stale, catching up.")

  private val startTime: DateTime = clock.now()

  @volatile private var liveVersion: Option[Long] = None
  @volatile private var processorVersion: Option[Long] = None

  @volatile private var currentHealth = ill
  @volatile private var currentState = staleReport
  @volatile private var finishTime: DateTime = null



  override def getReport: Report = currentState

  override def get(): State = currentHealth

  override def chaserReceived(version: Long): Unit = {
    currentState = staleReport
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
        if (finishTime == null) {
          finishTime = clock.now()
        }
        currentHealth = healthy
        currentState = caughtUpReport
      }
    }
  }

  override def eventProcessingFailed(e: Exception): Unit = {
    LoggerFactory.getLogger(getClass).warn("Event subscription terminated", e)
    currentState = new Report(Status.WARNING, "Event subscription terminated: " + e.getMessage)
  }

  override def transientFailure(e: Exception): Unit = {}
}
