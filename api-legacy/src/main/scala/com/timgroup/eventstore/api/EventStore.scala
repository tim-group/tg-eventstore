package com.timgroup.eventstore.api

import org.joda.time.DateTime

/**
  * @deprecated uaw EventSource instead
  */
@Deprecated
trait EventStore {
  def save(newEvents: Seq[EventData], expectedVersion: Option[Long] = None): Unit

  def fromAll(version: Long = 0): EventStream

  def streamingFromAll(version: Long = 0): java.util.stream.Stream[EventInStream]
}

/**
  * @deprecated uaw EventSource instead
  */
@Deprecated
trait EventStream extends Iterator[EventInStream]

/**
  * @deprecated uaw EventSource instead
  */
@Deprecated
case class Body(data: Array[Byte]) {
  override def equals(obj: scala.Any): Boolean = {
    if (obj != null && obj.getClass == getClass) {
      data.deep == obj.asInstanceOf[Body].data.deep
    } else {
      false
    }
  }
}

/**
  * @deprecated uaw EventSource instead
  */
@Deprecated
case class EventData(eventType: String, body: Body)

/**
  * @deprecated uaw EventSource instead
  */
@Deprecated
object EventData {
  def apply(eventType: String, body: Array[Byte]): EventData = EventData(eventType, Body(body))
}

/**
  * @deprecated uaw EventSource instead
  */
@Deprecated
case class EventInStream(effectiveTimestamp: DateTime,
                         eventData: EventData,
                         version: Long) {
  val position = LegacyPositionAdapter(version)
}

/**
  * @deprecated uaw EventSource instead
  */
@Deprecated
case class LegacyPositionAdapter(version: Long) extends Position {
  override def toString: String = version.toString
}