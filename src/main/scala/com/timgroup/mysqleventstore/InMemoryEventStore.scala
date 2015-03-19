package com.timgroup.mysqleventstore

import org.joda.time.{DateTimeZone, DateTime}

class InMemoryEventStore extends EventStore {
  var events = Vector[(DateTime, EventData)]()

  override def save(newEvents: Seq[EventData]): Unit = {

    events= events ++ newEvents.map { evt => (new DateTime(DateTimeZone.UTC), evt) }
  }

  override def fromAll(version: Long, batchSize: Option[Int] = None): EventPage = {
    val last = events.size

    val fetched = events.drop(version.toInt).take(batchSize.getOrElse(Int.MaxValue))

    EventPage(fetched.toIterator.zipWithIndex.map {
      case ((effectiveTimestamp, data), index) => EventInStream(effectiveTimestamp, data, index + 1, last)
    })
  }
}
