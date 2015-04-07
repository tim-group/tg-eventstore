package com.timgroup.mysqleventstore

import org.joda.time.DateTime

trait EventStore {
  def save(newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit

  def fromAll(version: Long = 0, batchSize: Option[Int] = None): EventPage
}

case class EventPage(events: Seq[EventInStream]) {
  def eventData: Seq[EventData] = events.map(_.eventData)

  def isEmpty = events.isEmpty

  def size = events.size

  def lastOption = events.lastOption

  def isLastPage = lastOption.map(_.last)
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
