package com.timgroup.mysqleventstore

import org.joda.time.DateTime

class InMemoryEventStore extends EventStore {
  var events = Vector[EffectiveEvent]()

  override def save(newEvents: Seq[SerializedEvent]): Unit = {
    events= events ++ newEvents.map(EffectiveEvent(new DateTime(), _)).zipWithIndex.map {
      case (evt, index) => evt.copy(version = Some(index + events.length + 1))
    }
  }

  override def fromAll(version: Long, batchSize: Option[Int] = None): EventStream = {
    val fetched = events.dropWhile(_.version.get <= version).take(batchSize.getOrElse(Int.MaxValue))

    val last = fetched.lastOption.flatMap(_.version)
    EventStream(fetched.dropWhile(_.version.get <= version).toIterator.map(_.copy(lastVersion = last)))
  }
}
