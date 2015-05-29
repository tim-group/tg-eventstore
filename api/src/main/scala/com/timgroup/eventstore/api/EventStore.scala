package com.timgroup.eventstore.api

import org.joda.time.DateTime

trait EventStore {
  def save(newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit

  def fromAll(version: Long = 0): EventStream

  def fromAll(version: Long, eventHandler: EventInStream => Unit): Unit
}

trait EventStream extends Iterator[EventInStream]

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
                         version: Long)