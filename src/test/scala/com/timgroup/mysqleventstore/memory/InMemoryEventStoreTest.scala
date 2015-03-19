package com.timgroup.mysqleventstore.memory

import com.timgroup.mysqleventstore.EventStoreTest
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class InMemoryEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
  val inMemoryEventStore = new InMemoryEventStore(
    now = () => effectiveTimestamp
  )

  override protected def afterEach(): Unit = {
    inMemoryEventStore.clear()
  }

  it should behave like anEventStore(inMemoryEventStore)
}
