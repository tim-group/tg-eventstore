package com.timgroup.eventstore.memory

import com.timgroup.clocks.joda.testing.ManualJodaClock
import com.timgroup.eventstore.api.EventStoreTest
import com.timgroup.eventstore.memory.Wrapper._
import org.joda.time.DateTimeZone
import org.scalatest.{FunSpec, MustMatchers, OneInstancePerTest}

class InMemoryEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with OneInstancePerTest {

  describe("traditional") {
    val traditionalInMemoryEventStore = new InMemoryEventStore( new ManualJodaClock(effectiveTimestamp.toInstant, DateTimeZone.UTC) )

    it should behave like anEventStore(traditionalInMemoryEventStore)

    it should behave like optimisticConcurrencyControl(traditionalInMemoryEventStore)
  }

  describe("wrapper around new") {
    val newInMemoryEventStore = new JavaInMemoryEventStore(new ManualJodaClock(effectiveTimestamp.toInstant, DateTimeZone.UTC)).toLegacy

    it should behave like anEventStore(newInMemoryEventStore)

    it should behave like optimisticConcurrencyControl(newInMemoryEventStore)
  }
}
