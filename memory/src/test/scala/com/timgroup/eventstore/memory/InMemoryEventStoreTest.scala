package com.timgroup.eventstore.memory

import com.timgroup.eventstore.api.{Clock, EventStoreTest}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class InMemoryEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
  val inMemoryEventStore = new InMemoryEventStore(
    now = new Clock {
      override def now(): DateTime = effectiveTimestamp
    }
  )

  override protected def afterEach(): Unit = {
    inMemoryEventStore.clear()
  }

  it should behave like anEventStore(inMemoryEventStore)

  it should behave like optimisticConcurrencyControl(inMemoryEventStore)
}
