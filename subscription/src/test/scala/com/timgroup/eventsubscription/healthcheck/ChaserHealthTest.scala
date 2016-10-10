package com.timgroup.eventsubscription.healthcheck

import java.time.{Clock, Instant}

import com.timgroup.eventstore.api.LegacyPositionAdapter
import com.timgroup.tucker.info.Status.{CRITICAL, OK, WARNING}
import org.joda.time.DateTime
import org.mockito.Mockito.{mock, when}
import org.scalatest.{FunSpec, MustMatchers}

class ChaserHealthTest extends FunSpec with MustMatchers {
  it("reports OK if chaser polled evenstore within last 5s") {
    val clock = mock(classOf[Clock])
    val now = Instant.now()
    val health = new ChaserHealth("", clock)

    when(clock.instant()).thenReturn(now)
    health.chaserUpToDate(LegacyPositionAdapter(134))

    when(clock.instant()).thenReturn(now.plusSeconds(4))

    health.getReport.getStatus must be(OK)
    health.getReport.getValue.toString must (endWith("Current version: 134"))
  }

  it("reports WARNING if chaser did not poll eventstore in over 5s") {
    val clock = mock(classOf[Clock])
    val now = Instant.now()
    val health = new ChaserHealth("", clock)

    when(clock.instant()).thenReturn(now)
    health.chaserUpToDate(LegacyPositionAdapter(1))

    when(clock.instant()).thenReturn(now.plusSeconds(6))

    health.getReport.getStatus must be(WARNING)
  }

  it("reports CRITICAL if chaser did not poll eventstore in over 30s") {
    val clock = mock(classOf[Clock])
    val now = Instant.now()
    val health = new ChaserHealth("", clock)

    when(clock.instant()).thenReturn(now)
    health.chaserUpToDate(LegacyPositionAdapter(1))

    when(clock.instant()).thenReturn(now.plusSeconds(31))

    health.getReport.getStatus must be(CRITICAL)
  }

  it("does not report CRITICAL if initial replay takes longer than 30s (still WARNING)") {
    val clock = mock(classOf[Clock])
    val now = Instant.now()
    val health = new ChaserHealth("", clock)

    when(clock.instant()).thenReturn(now)
    health.chaserReceived(LegacyPositionAdapter(1))

    when(clock.instant()).thenReturn(now.plusSeconds(31))

    health.getReport.getStatus must be(WARNING)
  }
}
