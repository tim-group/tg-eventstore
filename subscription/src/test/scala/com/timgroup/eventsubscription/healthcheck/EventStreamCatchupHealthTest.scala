package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventstore.api.{EventInStream, EventPage}
import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.tucker.info.Health.State
import com.timgroup.tucker.info.Status
import org.joda.time.DateTime
import org.mockito.Mockito._
import org.scalatest.{FunSpec, MustMatchers}

class EventStreamCatchupHealthTest extends FunSpec with MustMatchers {
  it("reports warning & ill if not caught up yet") {
    val component = new EventStreamCatchupHealth("", SystemClock)

    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 1)), 2))

    component.get() must be(State.ill)
    component.getReport.getStatus must be(Status.WARNING)
  }

  it("reports healthy & ok when caught up") {
    val component = new EventStreamCatchupHealth("", SystemClock)

    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 1)), 2))
    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 2)), 2))

    component.get() must be(State.healthy)
    component.getReport.getStatus must be(Status.OK)
  }

  it("reports warning if no longer up-to-date") {
    val component = new EventStreamCatchupHealth("", SystemClock)

    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 1)), 2))
    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 2)), 2))
    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 3)), 4))

    component.get() must be(State.healthy)
    component.getReport.getStatus must be(Status.WARNING)
  }

  it("reports the time taken for the system to catch up") {
    val startTime = new DateTime()
    val clock = mock(classOf[Clock])

    when(clock.now()).thenReturn(startTime)
    val component = new EventStreamCatchupHealth("", clock)

    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 1)), 2))
    when(clock.now()).thenReturn(startTime.plusSeconds(5))
    component.apply(EventPage(Seq(EventInStream(new DateTime(), null, 2)), 2))

    component.getReport.getValue must be("Caught up, version 2. Initial replay took 5 seconds.")
  }
}
