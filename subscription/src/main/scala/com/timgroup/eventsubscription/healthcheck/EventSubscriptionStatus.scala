package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Status.{CRITICAL, OK, WARNING}
import com.timgroup.tucker.info.{Component, Health, Report}
import org.joda.time.DateTime
import org.joda.time.Seconds.secondsBetween

class EventSubscriptionStatus(name: String, clock: Clock) extends Component("event-subscription-status-" + name, "Event subscription status (" + name + ")") with Health with SubscriptionListener {
  private val startTime: DateTime = clock.now()

  @volatile private var terminatedReport: Option[Report] = None

  @volatile private var staleSince: Option[DateTime] = None
  @volatile private var currentVersion: Option[Long] = None
  @volatile private var initialReplayDuration: Option[Int] = None

  override def getReport = terminatedReport.getOrElse {
    (staleSince, initialReplayDuration) match {
      case (Some(stale), _) => {
        val staleSeconds = secondsBetween(stale, clock.now()).getSeconds
        val status = if (staleSeconds > 30) { CRITICAL } else { WARNING }
        new Report(status, "Stale, catching up. " + currentVersion.map(v => "Currently at version " + v + ".").getOrElse("No events processed yet.") + " (Stale for " + staleSeconds +"s)")
      }
      case (None, Some(initialDuration)) => if (initialDuration < 240) {
        new Report(OK, "Caught up at version " + currentVersion.getOrElse("") + ". Initial replay took " + initialDuration + "s.")
      } else {
        new Report(WARNING, "Caught up at version " + currentVersion.getOrElse("") + ". Initial replay took " + initialDuration + "s. This is longer than expected limit of 240s.")
      }
      case (None, None) => new Report(WARNING, "Awaiting events.")
    }
  }

  override def get() = if (initialReplayDuration.isDefined) healthy else ill

  override def caughtUpAt(version: Long): Unit = {
    if (initialReplayDuration.isEmpty) {
      initialReplayDuration = Some(secondsBetween(startTime, clock.now()).getSeconds)
    }

    staleSince = None

    currentVersion = Some(version)
  }

  override def staleAtVersion(version: Option[Long]): Unit = {
    if (staleSince.isEmpty) {
      staleSince = Some(clock.now())
    }
    currentVersion = version
  }

  override def terminated(version: Long, e: Exception): Unit = {
    terminatedReport = Some(new Report(CRITICAL, "Event subscription terminated. Failed to process version " + version + ": " + e.getMessage + " at " + e.getStackTrace.apply(0)))
  }
}
