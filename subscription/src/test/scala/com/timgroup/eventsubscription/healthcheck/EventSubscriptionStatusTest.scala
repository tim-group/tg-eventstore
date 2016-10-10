package com.timgroup.eventsubscription.healthcheck

import java.time.{Clock, Instant}
import java.util.Collections

import com.timgroup.eventstore.api.LegacyPositionAdapter
import com.timgroup.tucker.info.Report
import com.timgroup.tucker.info.Status.{CRITICAL, OK}
import org.mockito.Mockito.{mock, when}
import org.scalatest.{FunSpec, MustMatchers, OneInstancePerTest}

class EventSubscriptionStatusTest extends FunSpec with MustMatchers with OneInstancePerTest {
  val timestamp = Instant.parse("2016-02-01T00:00:00.000Z")

  val clock = mock(classOf[Clock])
  when(clock.instant()).thenReturn(timestamp)
  val status = new EventSubscriptionStatus("", clock, 123)
  val adapter = new SubscriptionListenerAdapter(LegacyPositionAdapter(0), Collections.singletonList(status))

  it("reports failure if subscription terminates (due to deserialization failure)") {
    adapter.chaserReceived(LegacyPositionAdapter(1))
    adapter.chaserUpToDate(LegacyPositionAdapter(1))
    adapter.eventDeserializationFailed(LegacyPositionAdapter(1), new RuntimeException("Failure from deserialization"))

    status.getReport().getStatus must be(CRITICAL)
    status.getReport().getValue.asInstanceOf[String] must include("Event subscription terminated. Failed to process version 1: Failure from deserialization")
    status.getReport.getValue.asInstanceOf[String] must include("EventSubscriptionStatusTest")
  }
}
