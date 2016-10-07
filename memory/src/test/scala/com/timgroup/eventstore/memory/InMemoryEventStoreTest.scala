package com.timgroup.eventstore.memory

import java.time.{Clock, Instant, ZoneOffset}

import com.timgroup.eventstore.api.{EventStoreTest, Clock => LegacyClock}
import org.joda.time.DateTime
import org.scalatest.{FunSpec, MustMatchers, OneInstancePerTest}

class InMemoryEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with OneInstancePerTest {
  describe("traditional") {
    val traditionalInMemoryEventStore = new InMemoryEventStore(
      now = new LegacyClock {
        override def now(): DateTime = effectiveTimestamp
      }
    )

    it should behave like anEventStore(traditionalInMemoryEventStore)

    it should behave like optimisticConcurrencyControl(traditionalInMemoryEventStore)
  }

  describe("wrapper around new") {
    val newInMemoryEventStore = new JavaInMemoryEventStore(Clock.fixed(Instant.ofEpochMilli(effectiveTimestamp.getMillis), ZoneOffset.UTC)).toLegacy

    it should behave like anEventStore(newInMemoryEventStore)

    it should behave like optimisticConcurrencyControl(newInMemoryEventStore)
  }
}
