package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Status.{OK, WARNING}
import com.timgroup.tucker.info.{Component, Health, Report, Status}
import org.joda.time.DateTime
import org.joda.time.Seconds.secondsBetween

class EventSubscriptionStatus(name: String, clock: Clock) extends Component("event-subscription-status-" + name, "Event subscription status (" + name + ")") with Health with SubscriptionListener {
  private val startTime: DateTime = clock.now()

  @volatile private var initialReplayDuration: Option[Int] = None
  @volatile private var report = new Report(Status.WARNING, "Awaiting events.")
  @volatile private var terminatedReport: Option[Report] = None

  override def getReport = terminatedReport.getOrElse(report)

  override def get() = if (initialReplayDuration.isDefined) healthy else ill

  override def caughtUpAt(version: Long): Unit = {
    if (initialReplayDuration.isEmpty) {
      initialReplayDuration = Some(secondsBetween(startTime, clock.now()).getSeconds)
    }

    report = if (initialReplayDuration.get < 240) {
      new Report(OK, "Caught up at version " + version + ". Initial replay took " + initialReplayDuration.get + "s.")
    } else {
      new Report(WARNING, "Caught up at version " + version + ". Initial replay took " + initialReplayDuration.get + "s. This is longer than expected limit of 240s.")
    }
  }

  override def staleAtVersion(version: Option[Long]): Unit = {
    report = new Report(WARNING, "Stale, catching up. " + version.map(v => "Currently at version " + v + ".").getOrElse("No events processed yet."))
  }

  override def terminated(e: Exception): Unit = {
    terminatedReport = Some(new Report(Status.WARNING, "Event subscription terminated: " + e.getMessage))
  }
}
