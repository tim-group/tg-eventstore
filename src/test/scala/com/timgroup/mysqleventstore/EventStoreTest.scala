package com.timgroup.mysqleventstore

import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest._

trait EventStoreTest { this: FunSpec with MustMatchers =>
  val effectiveTimestamp = new DateTime(2015, 1, 15, 23, 43, 53, DateTimeZone.UTC)

  def anEventStore(eventStore: EventStore) = {
    it("can save events") {
      eventStore.save(serialized(ExampleEvent(21), ExampleEvent(22)))

      val all = eventStore.fromAll().events.toList

      all.map(_.effectiveTimestamp) must be(List(effectiveTimestamp, effectiveTimestamp))
      all.map(_.eventData).map(deserialize) must be(List(ExampleEvent(21),ExampleEvent(22)))
    }

    it("can replay events from a given version number onwards") {
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))

      val previousVersion = eventStore.fromAll().events.toList.last.version

      eventStore.save(serialized(ExampleEvent(3), ExampleEvent(4)))

      val nextEvents = eventStore.fromAll(version = previousVersion).events.toList.map(_.eventData).map(deserialize)

      nextEvents must be(List(ExampleEvent(3), ExampleEvent(4)))
    }

    it("returns no events if there are none past the specified version") {
      eventStore.fromAll(version = 900000).isEmpty must be(true)
    }

    it("labels the last event of the stream") {
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))

      val nextEvents = eventStore.fromAll().events.toList

      nextEvents.map(_.last) must be(List(false, true))
    }

    it("writes events to the current version of the stream when no expected version is specified") {
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))
      eventStore.save(serialized(ExampleEvent(3)))

      eventStore.fromAll(version = 2).events.toList.map(_.eventData).map(deserialize) must be(List(ExampleEvent(3)))
    }

    it("throws OptimisticConcurrencyFailure when stream has already moved beyond the expected version") {
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))
      intercept[OptimisticConcurrencyFailure] {
        eventStore.save(serialized(ExampleEvent(3)), expectedVersion = Some(1))
      }
    }

    it("throws OptimisticConcurrencyFailure when stream is not yet at the expected version") {
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2)))
      intercept[OptimisticConcurrencyFailure] {
        eventStore.save(serialized(ExampleEvent(3)), expectedVersion = Some(10))
      }
    }

    it("returns nothing if eventstore is empty") {
        eventStore.fromAll().eventData.toList must be(Nil)
    }

    it("returns only number of events asked for in batchSize") {
      eventStore.save(serialized(ExampleEvent(1), ExampleEvent(2), ExampleEvent(3), ExampleEvent(4)))

      eventStore.fromAll(0, Some(2)).events.toList.map(_.eventData).map(deserialize) must be(List(ExampleEvent(1), ExampleEvent(2)))
      eventStore.fromAll(2, Some(2)).events.toList.map(_.eventData).map(deserialize) must be(List(ExampleEvent(3), ExampleEvent(4)))
    }

    it("throws OptimisticConcurrencyFailure when stream moves past expected version during save") {
      eventStore.fromAll()

      unrelatedSavesOfEventHappens()
      intercept[OptimisticConcurrencyFailure] {
        eventStore.save(serialized(ExampleEvent(3)), expectedVersion = Some(0))
      }
    }

    def unrelatedSavesOfEventHappens(): Unit = {
      eventStore.save(serialized(ExampleEvent(3)))
    }
  }

  case class ExampleEvent(a: Int)

  def serialized(evts: ExampleEvent*) = evts.map(serialize)

  def serialize(evt: ExampleEvent) = EventData(evt.getClass.getSimpleName, evt.a.toString.getBytes("UTF-8"))

  def deserialize(evt: EventData) = ExampleEvent(new String(evt.body, "UTF-8").toInt)
}
