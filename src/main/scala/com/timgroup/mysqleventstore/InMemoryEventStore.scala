package com.timgroup.mysqleventstore

import org.joda.time.DateTime


class InMemoryEventStore extends EventStore {
  var events = Vector[EffectiveEvent]()

  override def saveLiveEvent(newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit = {
    val oldLast: Long = events.map(_.version).max
    val newLast: Long = oldLast + newEvents.length
    events = events ++ newEvents.zipWithIndex.map {
      case (evt, index) =>
        EffectiveEvent(new DateTime(), evt, oldLast + 1 + index, newLast)
    }
  }

  override def saveBackfillEvent(newEvents: Seq[BackfillEvent], expectedVersion: Option[Long] = None): Unit = {
    val last: Long = (events.map(_.version) ++ newEvents.map(_.version)).max
    events = events ++ newEvents.zipWithIndex.map {
      case (evt, index) => EffectiveEvent(new DateTime(), evt.eventData, evt.version, last)
    }
  }

  override def fromAll(version: Long, batchSize: Option[Int] = None): EventStream = {
    val last = events.lastOption.map(_.version)

    val fetched = events.dropWhile(_.version <= version).take(batchSize.getOrElse(Int.MaxValue))

    EventStream(fetched.dropWhile(_.version <= version).toIterator.map(_.copy(lastVersion = last.get)))
  }
}
