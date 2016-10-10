package com.timgroup.eventsubscription

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.function.Consumer

import com.lmax.disruptor.dsl.{Disruptor, ProducerType}
import com.lmax.disruptor.{BlockingWaitStrategy, EventFactory, EventTranslator, WorkHandler}
import com.timgroup.eventstore.api._
import com.timgroup.eventsubscription.healthcheck.{ChaserHealth, EventSubscriptionStatus, SubscriptionListener, SubscriptionListenerAdapter}
import com.timgroup.tucker.info.{Component, Health}


class EventSubscription[T](
            name: String,
            eventstore: EventReader,
            deserializer: Deserializer[T],
            handlers: java.util.List[EventHandler[T]],
            clock: Clock = SystemClock,
            bufferSize: java.lang.Integer,
            runFrequency: java.lang.Long,
            startingPosition: Position,
            maxInitialReplayDuration: java.lang.Integer,
            listeners: java.util.List[SubscriptionListener]) {

  private val chaserHealth = new ChaserHealth(name, clock)
  private val subscriptionStatus = new EventSubscriptionStatus(name, clock, maxInitialReplayDuration)

  import scala.collection.JavaConversions._
  private val subListeners: java.util.List[SubscriptionListener] = subscriptionStatus.asInstanceOf[SubscriptionListener] +: listeners
  private val processorListener = new SubscriptionListenerAdapter(startingPosition, subListeners)
  private val chaserListener = new BroadcastingChaserListener(chaserHealth, processorListener)

  val statusComponents: List[Component] = List(subscriptionStatus, chaserHealth)
  val health: Health = subscriptionStatus

  private val executor = Executors.newScheduledThreadPool(1, new ThreadFactory {
    private val count = new AtomicInteger()

    override def newThread(r: Runnable) = {
      val thread = new Thread(r, "EventChaser-" + name + "-" + count.getAndIncrement)
      thread.setDaemon(true)
      thread
    }
  })

  private val eventHandler = new BroadcastingEventHandler[T](handlers)

  private val eventHandlerExecutor = Executors.newCachedThreadPool(new ThreadFactory {
    private val count = new AtomicInteger()

    override def newThread(r: Runnable) = {
      val thread = new Thread(r, "EventSubscription-" + name + "-" + count.getAndIncrement)
      thread.setDaemon(true)
      thread
    }
  })

  private val disruptor = new Disruptor[EventContainer[T]](new EventContainer.Factory[T](), bufferSize, eventHandlerExecutor, ProducerType.SINGLE, new BlockingWaitStrategy())

  disruptor
    .handleEventsWithWorkerPool(
      new DisruptorDeserializationAdapter[T](deserializer, processorListener),
      new DisruptorDeserializationAdapter[T](deserializer, processorListener))
    .then(new DisruptorEventHandlerAdapter(eventHandler, processorListener))

  def start() {
    val chaser = new EventStoreChaser(eventstore, startingPosition, new Consumer[ResolvedEvent] {
      override def accept(evt: ResolvedEvent): Unit = disruptor.publishEvent(new SetEventInStream(evt))
    }, chaserListener)

    disruptor.start()

    executor.scheduleWithFixedDelay(chaser, 0, runFrequency, TimeUnit.MILLISECONDS)
  }

  def stop() {
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.SECONDS)
    disruptor.halt
    eventHandlerExecutor.shutdown()
    eventHandlerExecutor.awaitTermination(1, TimeUnit.SECONDS)
  }
}

class SetEventInStream[T](evt: ResolvedEvent) extends EventTranslator[EventContainer[T]] {
  override def translateTo(eventContainer: EventContainer[T], sequence: Long): Unit = eventContainer.event = evt
}
