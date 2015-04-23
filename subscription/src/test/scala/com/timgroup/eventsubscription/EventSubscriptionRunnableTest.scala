package com.timgroup.eventsubscription

import com.timgroup.eventstore.api._
import com.timgroup.eventstore.memory.InMemoryEventStore
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FunSpec

class EventSubscriptionRunnableTest extends FunSpec {
  it("invokes event handler for each event in the eventstore") {
    val handler1 = mock(classOf[EventHandler])
    val handler2 = mock(classOf[EventHandler])
    val eventstore = new InMemoryEventStore()

    eventstore.save(Seq(EventData("A", new Array[Byte](0)), EventData("B", new Array[Byte](0))))
    val runnable = new EventSubscriptionRunnable(eventstore, new BroadcastingEventHandler(List(handler1, handler2)))

    runnable.run()

    eventstore.save(Seq(EventData("C", new Array[Byte](0))))

    runnable.run()

    verify(handler1, times(3)).apply(Matchers.isA(classOf[EventInStream]))
    verify(handler2, times(3)).apply(Matchers.isA(classOf[EventInStream]))
  }

  it("notifies listener when eventstore throws an exception") {
    val failingES = mock(classOf[EventStore])
    val listener = mock(classOf[EventSubscriptionListener])

    val exception = new RuntimeException("Failed to poll")
    when(failingES.fromAll(0)).thenReturn(new EventStream {
      override def next(): EventInStream = ???

      override def hasNext: Boolean = throw exception
    })

    new EventSubscriptionRunnable(failingES, null, listener, None).run()

    verify(listener).pollFailed(exception)
  }

  it("propagates exception and notifies listener on event handler failure") {
    val eventstore = new InMemoryEventStore()
    val listener = mock(classOf[EventSubscriptionListener])

    val exception = new RuntimeException("Failed to handle this event!")

    eventstore.save(Seq(EventData("Event", Body(Array()))))

    val failingEventHandler = new EventHandler { override def apply(event: EventInStream) = throw exception }

    intercept[RuntimeException] {
      new EventSubscriptionRunnable(eventstore, failingEventHandler, listener, None).run()
    }

    verify(listener).eventHandlerFailure(exception)
  }
}
