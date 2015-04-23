package com.timgroup.eventsubscription

import java.util.concurrent.{ArrayBlockingQueue, Semaphore, TimeUnit}

import com.timgroup.eventstore.api.{Body, EventData, EventInStream}
import com.timgroup.eventstore.memory.InMemoryEventStore
import com.timgroup.eventsubscription.EventSubscriptionManager.SubscriptionSetup
import com.timgroup.eventsubscription.util.Clock
import com.timgroup.tucker.info.Health.State.{healthy, ill}
import com.timgroup.tucker.info.Report
import com.timgroup.tucker.info.Status.{OK, WARNING}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone._
import org.mockito.Mockito._
import org.mockito.{Matchers, Mockito}
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

import scala.util.Random

class EndToEndTest extends FunSpec with MustMatchers with BeforeAndAfterEach {
  var setup: SubscriptionSetup = _

  it("reports ill until caught up") {
    val store = new InMemoryEventStore()
    val eventProcessing = new BlockingEventHandler

    store.save(List(anEvent(), anEvent(), anEvent()))

    val cycle = new CycleListener()
    setup = EventSubscriptionManager("test", store, List(eventProcessing), listener = cycle)
    setup.subscriptionManager.start()

    setup.health.get() must be(ill)

    eventProcessing.allowProcessing(3)
    cycle.awaitInitialReplayCompletion()

    setup.health.get() must be(healthy)
  }

  it("reports current version") {
    val store = new InMemoryEventStore()
    store.save(List(anEvent(), anEvent(), anEvent()))

    val cycle = new CycleListener()
    setup = EventSubscriptionManager("test", store, List(), listener = cycle)
    setup.subscriptionManager.start()
    val component = setup.components.find(_.getId == "event-stream-version-test").get

    cycle.awaitInitialReplayCompletion()
    component.getReport.getValue must be(3)
  }

  it("reports warning if event store was not polled recently") {
    val initialTime = new DateTime(2015, 2, 20, 15, 21, 50, UTC)
    val clock = mock(classOf[Clock])
    when(clock.now()).thenReturn(initialTime)

    val cycle = new CycleListener()
    setup = EventSubscriptionManager("test", new InMemoryEventStore(), Nil, clock, listener = cycle)
    setup.subscriptionManager.start()

    cycle.awaitInitialReplayCompletion()

    val component = setup.components.find(_.getId == "event-store-polling-test").get

    component.getReport.getStatus must be(OK)
    setup.subscriptionManager.stop()
    when(clock.now()).thenReturn(initialTime.plusSeconds(6))
    component.getReport.getStatus must be(WARNING)
  }

  it("reports failure when event subscription terminates due to an eventhandler failure") {
    val store = new InMemoryEventStore()
    val failingHandler = mock(classOf[EventHandler])

    Mockito.doThrow(new RuntimeException("failure")).when(failingHandler).apply(Matchers.any())

    store.save(List(anEvent()))

    val cycle = new CycleListener()
    setup = EventSubscriptionManager("test", store, List(failingHandler), listener = cycle)
    setup.subscriptionManager.start()

    cycle.awaitFailure()

    val component = setup.components.find(_.getId == "event-subscription-status-test").get

    component.getReport must be(new Report(WARNING, "Event subscription terminated: failure"))
  }


  override protected def afterEach(): Unit = {
    setup.subscriptionManager.stop()
  }

  def anEvent() = EventData("A", Body(Random.alphanumeric.take(10).mkString.getBytes("utf-8")))
}

class BlockingEventHandler extends EventHandler {
  val lock = new Semaphore(0)

  override def apply(event: EventInStream): Unit = {
    lock.acquire()
  }

  def allowProcessing(count: Int) {
    lock.release(count)
  }
}

class CycleListener extends EventSubscriptionListener {
  val count = new ArrayBlockingQueue[Unit](1000)

  private val replayCompleted = new Semaphore(0)
  private val failureHappened = new Semaphore(0)

  override def pollSucceeded(): Unit = {}

  override def cycleCompleted(): Unit = { count.add(Unit) }

  override def eventHandlerFailure(e: Exception): Unit = { failureHappened.release() }

  override def pollFailed(e: Exception): Unit = {}

  override def initialReplayCompleted(): Unit = {
    replayCompleted.release()
  }

  override def newEventsFound(): Unit = {}

  override def caughtUp(): Unit = {}

  override def eventSubscriptionStarted(): Unit = {}

  def await() = count.poll(1, TimeUnit.SECONDS)

  def awaitInitialReplayCompletion(): Unit = {
    replayCompleted.acquire()
  }

  def awaitFailure(): Unit = {
    failureHappened.acquire()
  }
}