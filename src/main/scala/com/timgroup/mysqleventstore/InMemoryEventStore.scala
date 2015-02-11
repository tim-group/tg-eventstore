package com.timgroup.mysqleventstore

import org.joda.time.DateTime

class InMemoryEventStore extends EventStore {
  var events = Vector[EffectiveEvent]()

  override def save(newEvents: Seq[SerializedEvent]): Unit = {
    events= events ++ newEvents.map(EffectiveEvent(new DateTime(), _)).zipWithIndex.map {
      case (evt, index) => evt.copy(version = Some(index + events.length + 1))
    }
  }

  override def fromAll(version: Long): EventStream = {
    val last = events.lastOption.flatMap(_.version)
    EventStream(events.dropWhile(_.version.get <= version).toIterator.map(_.copy(lastVersion = last)))
  }
}
