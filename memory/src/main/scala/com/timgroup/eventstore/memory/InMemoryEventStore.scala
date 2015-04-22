package com.timgroup.eventstore.memory

import com.timgroup.eventstore.api._
import org.joda.time.{DateTime, DateTimeZone}

class InMemoryEventStore(now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) extends EventStore {
  var events = Vector[EventAtATime]()


  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit =  {
    if (expectedVersion.exists(_ != events.size)) {
      throw new OptimisticConcurrencyFailure()
    }
    events= events ++ newEvents.map { evt => EventAtATime(now(), evt) }
  }

  override def fromAll(version: Long, maybeBatchSize: Option[Int] = None): EventPage = {
    val last = events.size
    val batchSize = maybeBatchSize.getOrElse(Int.MaxValue)

    val potential = events.zipWithIndex.drop(version.toInt)
    val fetched = if (potential.length > batchSize) {
      potential.take(batchSize)
    } else {
      potential
    }

    EventPage(fetched.map {
      case (EventAtATime(effectiveTimestamp, data), index) => EventInStream(effectiveTimestamp, data, index + 1)
    }, last)
  }

  def clear(): Unit = {
    events = Vector()
  }
}
