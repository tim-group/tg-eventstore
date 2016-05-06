package com.timgroup.eventstore.vector

import com.timgroup.eventstore.api._
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
      val iterator = new CombinedIterator[EventInStream](iterators.toList)(EffectiveEventOrdering)
      iterator.foreach( event => eventHandler(event))
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

  def time(t: Int) = {
    new Clock {
      override def now(): DateTime = new DateTime(0L).plusHours(t)
    }
  }

  def someData(dat: Int) = Seq[EventData](new EventData("Example", Body(dat.toString.getBytes)))

  it("consumes one iteration of all event streams") {
    val eventstore1 = new InMemoryEventStore()
    val eventstore2 = new InMemoryEventStore()
    val checkpointVector = CheckPointVector.build(List(eventstore1, eventstore2))

    eventstore1.saveWithTime(time(1), someData(1), None)
    eventstore2.saveWithTime(time(2), someData(2), None)
    eventstore1.saveWithTime(time(3), someData(3), None)
    eventstore2.saveWithTime(time(4), someData(4), None)

    var eventsReceived = List[EventInStream]()
    val eventHandler: (EventInStream => Unit) = { eis =>
      eventsReceived = eventsReceived :+ eis
    }

    checkpointVector.executeBatch(eventHandler)

    eventsReceived.map {eis => new String(eis.eventData.body.data)} must be(List("1","2","3","4"))
  }

  it("finds more on the second run") {
    val eventstore1 = new InMemoryEventStore()
    val eventstore2 = new InMemoryEventStore()
    val checkpointVector = CheckPointVector.build(List(eventstore1, eventstore2))

    var eventsReceived = List[EventInStream]()
    val eventHandler: (EventInStream => Unit) = { eis =>
      eventsReceived = eventsReceived :+ eis
    }

    checkpointVector.executeBatch(eventHandler)

    eventsReceived.map {eis => new String(eis.eventData.body.data)} must be(List.empty)

    eventstore1.saveWithTime(time(1), someData(1), None)
    eventstore2.saveWithTime(time(2), someData(2), None)
    eventstore1.saveWithTime(time(3), someData(3), None)
    eventstore2.saveWithTime(time(4), someData(4), None)

    checkpointVector.executeBatch(eventHandler)

    eventsReceived.map {eis => new String(eis.eventData.body.data)} must be(List("1","2","3","4"))
  }


}