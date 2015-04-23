package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventstore.api.EventInStream
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.{Report, Status}
import org.joda.time.DateTime
import org.scalatest.{FunSpec, MustMatchers}

class EventSubscriptionStatusTest extends FunSpec with MustMatchers {
  it("initially reports ill") {
    new EventSubscriptionStatus("").get() must be(ill)
  }

  it("reports ill whilst initial replay is in progress") {
    val status = new EventSubscriptionStatus("")

    status.eventSubscriptionStarted()

    status.get() must be(ill)
    status.getReport() must be(new Report(Status.WARNING, "Event subscription started"))
  }

  it("reports healthy once initial replay is completed") {
    val status = new EventSubscriptionStatus("")

    status.eventSubscriptionStarted()
    status.initialReplayCompleted()

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.OK, "Caught up"))
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
    status.getReport() must be(new Report(Status.OK, "Caught up"))
  }

  it("reports failure if subscription terminates") {
    val status = new EventSubscriptionStatus("")

    status.eventSubscriptionStarted()
    status.initialReplayCompleted()
    status.eventHandlerFailure(new RuntimeException("Failure from handler"))

    status.getReport() must be(new Report(Status.WARNING, "Event subscription terminated: Failure from handler"))
  }
}
