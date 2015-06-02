package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.{Report, Status}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.mockito.Mockito.{mock, when}
import org.scalatest.{FunSpec, MustMatchers}

class EventSubscriptionStatusTest extends FunSpec with MustMatchers {
  it("reports ill whilst initial replay is in progress") {
    val status = new EventSubscriptionStatus("")

    status.get() must be(ill)
    status.getReport() must be(new Report(Status.WARNING, "Stale, catching up."))
  }

  it("reports healthy once initial replay is completed") {
    val timestamp = new DateTime(2014, 2, 1, 0, 0, 0, UTC)

    val clock = mock(classOf[Clock])
    when(clock.now()).thenReturn(timestamp)

    val status = new EventSubscriptionStatus("", clock)

    status.chaserReceived(1)
    status.chaserReceived(2)
    status.chaserReceived(3)
    status.chaserUpToDate(3)

    status.get() must be(ill)
    status.getReport() must be(new Report(Status.WARNING, "Stale, catching up."))

    when(clock.now()).thenReturn(timestamp.plusSeconds(100))
    status.eventProcessed(1)
    status.eventProcessed(2)
    status.eventProcessed(3)


    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.OK, "Caught up. Initial replay took 100s"))
  }

  it("reports warning if initial replay took longer than 240s") {
    val timestamp = new DateTime(2014, 2, 1, 0, 0, 0, UTC)

    val clock = mock(classOf[Clock])
    when(clock.now()).thenReturn(timestamp)

    val status = new EventSubscriptionStatus("", clock)

    status.chaserReceived(1)
    status.chaserReceived(2)
    status.chaserReceived(3)
    status.chaserUpToDate(3)

    status.get() must be(ill)
    status.getReport() must be(new Report(Status.WARNING, "Stale, catching up."))

    when(clock.now()).thenReturn(timestamp.plusSeconds(241))
    status.eventProcessed(1)
    status.eventProcessed(2)
    status.eventProcessed(3)

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.WARNING, "Caught up. Initial replay took 241s. This is longer than expected limit of 240s."))
  }

  it("reports warning if stale") {
    val status = new EventSubscriptionStatus("")

    status.chaserUpToDate(5)
    status.eventProcessed(5)
    status.chaserReceived(6)

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.WARNING, "Stale, catching up."))
  }

  it("reports OK once caught up again")  {
    val status = new EventSubscriptionStatus("")

    status.chaserReceived(1)
    status.chaserReceived(2)
    status.chaserUpToDate(2)
    status.eventProcessed(1)
    status.eventProcessed(2)

    status.get() must be(healthy)
    status.getReport().getStatus must be(Status.OK)
  }

  it("reports failure if subscription terminates") {
    val status = new EventSubscriptionStatus("")

    status.chaserReceived(1)
    status.chaserUpToDate(1)
    status.eventProcessingFailed(new RuntimeException("Failure from handler"))

    status.getReport() must be(new Report(Status.WARNING, "Event subscription terminated: Failure from handler"))
  }
}
