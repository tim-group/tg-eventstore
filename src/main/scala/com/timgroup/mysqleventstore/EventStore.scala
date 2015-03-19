package com.timgroup.mysqleventstore

import org.joda.time.DateTime

trait EventStore {
  def save(newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit

  def fromAll(version: Long = 0, batchSize: Option[Int] = None): EventPage
}

case class EventPage(events: Iterator[EventInStream]) {
  def eventData: Iterator[EventData] = events.map(_.eventData)

  def isEmpty = events.isEmpty
}

case class EventData(eventType: String, body: Array[Byte])

case class EventInStream(effectiveTimestamp: DateTime,
                         eventData: EventData,
                         version: Long,
                         lastVersion: Long) {
  val last = version == lastVersion
}
