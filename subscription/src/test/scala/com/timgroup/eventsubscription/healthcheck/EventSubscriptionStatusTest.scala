package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.{Report, Status}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.mockito.Mockito.{mock, when}
import org.scalatest.{FunSpec, MustMatchers}

class EventSubscriptionStatusTest extends FunSpec with MustMatchers {
  it("initially reports ill") {
    new EventSubscriptionStatus("").get() must be(ill)
  }

  it("reports ill whilst initial replay is in progress") {
    val status = new EventSubscriptionStatus("")

    status.eventSubscriptionStarted()

    status.get() must be(ill)
    status.getReport() must be(new Report(Status.WARNING, "Event subscription started. Catching up."))
  }

  it("reports healthy once initial replay is completed") {
    val timestamp = new DateTime(2014, 2, 1, 0, 0, 0, UTC)

    val clock = mock(classOf[Clock])
    val status = new EventSubscriptionStatus("", clock)

    when(clock.now()).thenReturn(timestamp)
    status.eventSubscriptionStarted()
    when(clock.now()).thenReturn(timestamp.plusSeconds(314))
    status.initialReplayCompleted()

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.OK, "Caught up. Initial replay took 314s"))
  }

  it("reports warning if stale") {
    val status = new EventSubscriptionStatus("")

    status.eventSubscriptionStarted()
    status.initialReplayCompleted()
    status.newEventsFound()

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.WARNING, "Stale"))
  }

  it("reports OK once caught up again")  {
    val status = new EventSubscriptionStatus("")

    status.eventSubscriptionStarted()
    status.initialReplayCompleted()
    status.newEventsFound()
    status.caughtUp()

    status.get() must be(healthy)
    status.getReport().getStatus must be(Status.OK)
  }

  it("reports failure if subscription terminates") {
    val status = new EventSubscriptionStatus("")

    status.eventSubscriptionStarted()
    status.initialReplayCompleted()
    status.eventHandlerFailure(new RuntimeException("Failure from handler"))

    status.getReport() must be(new Report(Status.WARNING, "Event subscription terminated: Failure from handler"))
  }
}
