package com.timgroup.eventstore.vector

import com.timgroup.eventstore.api.{EventData, EventStream, EventInStream, EventStore}
import com.timgroup.eventstore.memory.InMemoryEventStore
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class CheckPointVectorTest extends FunSpec with MustMatchers with BeforeAndAfterEach {

  case class CheckPoint(eventstore: EventStore, checkPoint: Option[Long] = None) {
    def nextIterator = eventstore.fromAll(checkPoint.getOrElse(0))
    def iterator = new CheckPointRecordingIterator(this)
  }

  case class CheckPointVector(var checkpoints: Seq[CheckPoint]) {
    def executeBatch(eventHandler: (EventInStream => Unit)): Unit = {
      val iterators = checkpoints.map(_.iterator)
      //val iterator = new CombinedIterator[EventInStream](iterators)(EffectiveEventOrdering)
      //iterator.foreach( x => f(x))
      this.checkpoints = iterators.map(_.newCheckpoint)
    }
  }

  object CheckPointVector {
    def build(eventstores: List[EventStore]) = {
      CheckPointVector(eventstores.map(eventstore => CheckPoint(eventstore)))
    }
  }

  class CheckPointRecordingIterator(checkPoint: CheckPoint) extends Iterator[EventInStream] {
    val underlying = checkPoint.nextIterator
    var checkPointPosition = checkPoint.checkPoint

    override def hasNext: Boolean = underlying.hasNext

    override def next(): EventInStream = {
      val next = underlying.next()
      checkPointPosition = Some(next.version)
      next
    }

    def newCheckpoint = CheckPoint(checkPoint.eventstore, checkPointPosition)
  }


  def f(eis: EventInStream) = {println(eis)}
//        checkpointVector.executeBatch


  it("consumes one iteration of all event streams") {

    new EventStore {override def save(newEvents: Seq[EventData], expectedVersion: Option[Long]): Unit = ???
      override def fromAll(version: Long): EventStream = ???
      override def fromAll(version: Long, eventHandler: (EventInStream) => Unit): Unit = ???
    }

    val e1 = EventInStream(DateTime.parse(""), null, 0)

    val eventstore1: EventStore = new InMemoryEventStore()
    val eventstore2: EventStore = new InMemoryEventStore()
    val checkpointVector = CheckPointVector.build(List(eventstore1, eventstore2))

    var eventsReceived = List()
    val eventHandler: (EventInStream => Unit) = { eis =>
      eventsReceived :+ eis
    }

    checkpointVector.executeBatch(eventHandler)
  }

}