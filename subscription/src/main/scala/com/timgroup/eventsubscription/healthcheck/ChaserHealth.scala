package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.ChaserListener
import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Status.{OK, WARNING, CRITICAL}
import com.timgroup.tucker.info.{Component, Report, Status}
import org.joda.time.{DateTime, Seconds}
import org.slf4j.LoggerFactory

class ChaserHealth(name: String, clock: Clock) extends Component("event-store-chaser-" + name, "Eventstore chaser health (" + name + ")") with ChaserListener {
  @volatile private var lastPollTimestamp: Option[DateTime] = None
  @volatile private var currentVersion: Long = 0

  override def getReport = {
    lastPollTimestamp match {
      case None => new Report(WARNING, "Awaiting initial catchup. Current version: " + currentVersion)
      case Some(timestamp) => {
        val seconds = Seconds.secondsBetween(timestamp, clock.now()).getSeconds
        if (seconds > 30) {
          new Report(CRITICAL, "potentially stale. Last up-to-date at at %s. (%s seconds ago).".format(timestamp, seconds))
        } else if (seconds > 5) {
          new Report(WARNING, "potentially stale. Last up-to-date at at %s. (%s seconds ago).".format(timestamp, seconds))
        } else {
          new Report(OK, "up-to-date at at %s. (%s seconds ago). Current version: %s".format(timestamp, seconds, currentVersion))
        }
      }
    }
  }

  override def transientFailure(e: Exception): Unit = {
    LoggerFactory.getLogger(getClass).warn("Failure chasing eventstream.", e)
  }

  override def chaserReceived(version: Long): Unit = {
    currentVersion = version
  }

  override def chaserUpToDate(version: Long): Unit = {
    lastPollTimestamp = Some(clock.now())
  }
}
