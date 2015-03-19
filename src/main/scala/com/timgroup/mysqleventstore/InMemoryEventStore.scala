package com.timgroup.mysqleventstore

import org.joda.time.{DateTimeZone, DateTime}

class InMemoryEventStore extends EventStore {
  var events = Vector[EventAtATime]()


  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit =  {
    events= events ++ newEvents.map { evt => EventAtATime(new DateTime(DateTimeZone.UTC), evt) }
  }

  override def fromAll(version: Long, batchSize: Option[Int] = None): EventPage = {
    val last = events.size

    val fetched = events.drop(version.toInt).take(batchSize.getOrElse(Int.MaxValue))

    EventPage(fetched.toIterator.zipWithIndex.map {
      case (EventAtATime(effectiveTimestamp, data), index) => EventInStream(effectiveTimestamp, data, index + 1, last)
    })
  }
}
