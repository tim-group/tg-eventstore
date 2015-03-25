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

case class Body(data: Array[Byte]) {
  override def equals(obj: scala.Any): Boolean = {
    if (obj != null && obj.getClass == getClass) {
      data.deep == obj.asInstanceOf[Body].data.deep
    } else {
      false
    }
  }
}

case class EventData(eventType: String, body: Body)

object EventData {
  def apply(eventType: String, body: Array[Byte]): EventData = EventData(eventType, Body(body))
}

case class EventInStream(effectiveTimestamp: DateTime,
                         eventData: EventData,
                         version: Long,
                         lastVersion: Long) {
  val last = version == lastVersion
}
