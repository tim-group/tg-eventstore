package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventstore.api.EventPage
import com.timgroup.eventsubscription.EventHandler
import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State
import com.timgroup.tucker.info.{Component, Health, Report, Status}
import org.joda.time.{DateTime, Seconds}

class EventStreamCatchupHealth(name: String, clock: Clock) extends Component("event-stream-catchup-" + name, "Event stream catchup (" + name + ")") with Health with EventHandler {
  private val startTimestamp = clock.now()
  @volatile private var replayFinishTimestamp: Option[DateTime] = None
  @volatile private var state = State.ill
  @volatile private var report = new Report(Status.WARNING, "No events received yet")

  override def get() = state

  override def apply(event: EventPage): Unit = {
    if (event.isLastPage.getOrElse(true)) {
      if (replayFinishTimestamp.isEmpty) {
        replayFinishTimestamp = Some(clock.now())
      }
      state = State.healthy
      val secondsTaken: Int = Seconds.secondsBetween(startTimestamp, replayFinishTimestamp.get).getSeconds
      report = new Report(Status.OK, "Caught up, version %s. Initial replay took %s seconds.".format(event.lastOption.map(_.version).getOrElse(""), secondsTaken))
    } else {
      report = new Report(Status.WARNING, "Not up-to-date, processing " + event.lastOption.map(_.version).getOrElse(""))
    }
  }

  override def getReport: Report = report
}
