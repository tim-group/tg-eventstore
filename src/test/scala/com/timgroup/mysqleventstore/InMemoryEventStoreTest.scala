package com.timgroup.mysqleventstore

import org.scalatest.{BeforeAndAfterEach, MustMatchers, FunSpec, FunSuite}

class InMemoryEventStoreTest extends FunSpec with EventStoreTest with MustMatchers with BeforeAndAfterEach {
  val inMemoryEventStore = new InMemoryEventStore(
    now = () => effectiveTimestamp
  )

  override protected def afterEach(): Unit = {
    inMemoryEventStore.clear()
  }

  it should behave like anEventStore(inMemoryEventStore)
}
