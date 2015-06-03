package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.{Report, Status}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.mockito.Mockito.{mock, when}
import org.scalatest.{OneInstancePerTest, FunSpec, MustMatchers}

class EventSubscriptionStatusTest extends FunSpec with MustMatchers with OneInstancePerTest {
  val timestamp = new DateTime(2014, 2, 1, 0, 0, 0, UTC)

  val clock = mock(classOf[Clock])
  when(clock.now()).thenReturn(timestamp)
  val status = new EventSubscriptionStatus("", clock)
  val adapter = new SubscriptionListenerAdapter(status)

  it("reports ill whilst initial replay is in progress") {
    status.get() must be(ill)
    status.getReport() must be(new Report(Status.WARNING, "Awaiting events."))
  }

  it("reports healthy once initial replay is completed") {
    adapter.chaserReceived(1)
    adapter.chaserReceived(2)
    adapter.chaserReceived(3)
    adapter.chaserUpToDate(3)

    status.get() must be(ill)
    status.getReport() must be(new Report(Status.WARNING, "Stale, catching up. No events processed yet."))

    when(clock.now()).thenReturn(timestamp.plusSeconds(100))
    adapter.eventProcessed(1)
    adapter.eventProcessed(2)
    adapter.eventProcessed(3)

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.OK, "Caught up at version 3. Initial replay took 100s."))
  }

  it("reports warning if initial replay took longer than 240s") {
    adapter.chaserReceived(1)
    adapter.chaserReceived(2)
    adapter.chaserReceived(3)
    adapter.chaserUpToDate(3)

    when(clock.now()).thenReturn(timestamp.plusSeconds(241))
    adapter.eventProcessed(1)
    adapter.eventProcessed(2)
    adapter.eventProcessed(3)

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.WARNING, "Caught up at version 3. Initial replay took 241s. This is longer than expected limit of 240s."))
  }

  it("reports warning if stale") {
    adapter.chaserUpToDate(5)
    adapter.eventProcessed(5)
    adapter.chaserReceived(6)

    status.get() must be(healthy)
    status.getReport() must be(new Report(Status.WARNING, "Stale, catching up. Currently at version 5."))
  }

  it("reports OK once caught up again")  {
    adapter.chaserReceived(1)
    adapter.chaserReceived(2)
    adapter.chaserUpToDate(2)
    adapter.eventProcessed(1)
    adapter.eventProcessed(2)

    status.get() must be(healthy)
    status.getReport().getStatus must be(Status.OK)
  }

  it("reports failure if subscription terminates") {
    adapter.chaserReceived(1)
    adapter.chaserUpToDate(1)
    adapter.eventProcessingFailed(new RuntimeException("Failure from handler"))

    status.getReport() must be(new Report(Status.WARNING, "Event subscription terminated: Failure from handler"))
  }
}
