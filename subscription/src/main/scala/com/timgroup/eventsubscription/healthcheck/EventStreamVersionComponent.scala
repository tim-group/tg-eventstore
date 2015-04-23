package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventstore.api.EventInStream
import com.timgroup.eventsubscription.EventHandler
import com.timgroup.tucker.info.Status.INFO
import com.timgroup.tucker.info.{Component, Report}

class EventStreamVersionComponent(name: String) extends Component("event-stream-version-" + name, "Event stream version (" + name + ")") with EventHandler {
  @volatile private var currentVersion: Option[Long] = None

  override def getReport: Report = new Report(INFO, currentVersion.getOrElse(""))

  override def apply(event: EventInStream): Unit = currentVersion = Some(event.version)
}
