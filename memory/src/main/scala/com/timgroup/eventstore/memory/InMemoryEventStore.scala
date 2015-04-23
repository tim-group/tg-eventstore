package com.timgroup.eventstore.memory

import com.timgroup.eventstore.api._
import org.joda.time.{DateTime, DateTimeZone}

class InMemoryEventStore(now: () => DateTime = () => DateTime.now(DateTimeZone.UTC)) extends EventStore {
  var events = Vector[EventInStream]()


  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit =  {
    val currentVersion = events.size

    if (expectedVersion.exists(_ != currentVersion)) {
      throw new OptimisticConcurrencyFailure()
    }

    events = events ++ newEvents.zipWithIndex.map { case (evt, index) => EventInStream(now(), evt, currentVersion + index + 1) }
  }

  override def fromAll(version: Long): EventStream = new EventStream {
    private var currentVersion: Int = version.toInt

    override def next(): EventInStream = {
      val event = events(currentVersion)
      currentVersion = currentVersion + 1
      event
    }

    override def hasNext: Boolean = events.size > currentVersion
  }

  def clear(): Unit = {
    events = Vector()
  }
}
