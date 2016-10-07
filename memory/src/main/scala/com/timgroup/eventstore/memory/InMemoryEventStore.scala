package com.timgroup.eventstore.memory

import java.util.stream.Stream

import com.timgroup.eventstore.api._

class InMemoryEventStore(now: Clock = SystemClock) extends EventStore {
  var events = Vector[EventInStream]()

  def saveWithTime(now: Clock = SystemClock, newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit =  {
    val currentVersion = events.size

    if (expectedVersion.exists(_ != currentVersion)) {
      throw new OptimisticConcurrencyFailure(None)
    }

    events = events ++ newEvents.zipWithIndex.map { case (evt, index) => EventInStream(now.now(), evt, currentVersion + index + 1) }
  }

  override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit =  {
    saveWithTime(now, newEvents, expectedVersion)
  }

  override def fromAll(version: Long): EventStream = new EventStream {
    private var currentVersion: Int = version.toInt
    private var hadNext = true

    override def next(): EventInStream = {
      val event = events(currentVersion)
      currentVersion = currentVersion + 1
      event
    }

    override def hasNext: Boolean = {
      if (hadNext) {
        hadNext = events.size > currentVersion
      }
      hadNext
    }
  }

  override def streamingFromAll(version: Long): Stream[EventInStream] = {
    scala.collection.JavaConversions.seqAsJavaList(fromAll(version).toList).stream()
  }

  def clear(): Unit = {
    events = Vector()
  }
}
