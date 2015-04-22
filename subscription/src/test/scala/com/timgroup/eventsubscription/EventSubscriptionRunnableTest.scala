package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.{Body, EventData, EventPage, EventStore}
import com.timgroup.eventstore.memory.InMemoryEventStore
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FunSpec

class EventSubscriptionRunnableTest extends FunSpec {
  it("notifies event handlers with each event in the eventstore") {
    val handler1 = mock(classOf[EventHandler])
    val handler2 = mock(classOf[EventHandler])
    val eventstore = new InMemoryEventStore()

    eventstore.save(Seq(EventData("A", new Array[Byte](0)), EventData("A", new Array[Byte](0))))
    val runnable = new EventSubscriptionRunnable(eventstore, new BroadcastingEventHandler(List(handler1, handler2)))

    runnable.run()

    verify(handler1, times(1)).apply(Matchers.isA(classOf[EventPage]))
    verify(handler2, times(1)).apply(Matchers.isA(classOf[EventPage]))
  }

  it("notifies event handlers in one run when there are more events in eventstore than the specified batch size") {
    val handler = mock(classOf[EventHandler])
    val eventstore = new InMemoryEventStore()

    eventstore.save(Seq(
      EventData("A", new Array[Byte](0)),
      EventData("A", new Array[Byte](0)),
      EventData("A", new Array[Byte](0))))

    val runnable = new EventSubscriptionRunnable(eventstore, handler, batchSize = Some(2))

    runnable.run()

    verify(handler, times(2)).apply(Matchers.isA(classOf[EventPage]))
  }

  it("notifies polling failure") {
    val failingES = mock(classOf[EventStore])
    val listener = mock(classOf[EventSubscriptionListener])

    val exception = new scala.RuntimeException("Failed to poll")
    when(failingES.fromAll(0, None)).thenThrow(exception)

    new EventSubscriptionRunnable(failingES, null, listener, None).run()

    verify(listener).pollFailed(exception)
  }

  it("propagates exception and notifies listener on event handler failure") {
    val eventstore = new InMemoryEventStore()
    val listener = mock(classOf[EventSubscriptionListener])

    val exception = new RuntimeException("Failed to handle this event!")

    eventstore.save(Seq(EventData("Event", Body(Array()))))

    val failingEventHandler = new EventHandler { override def apply(event: EventPage) = throw exception }

    intercept[RuntimeException] {
      new EventSubscriptionRunnable(eventstore, failingEventHandler, listener, None).run()
    }

    verify(listener).eventHandlerFailure(exception)
  }
}
