package com.timgroup.eventsubscription

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.{EventFactory, EventTranslator, WorkHandler}
import com.timgroup.eventstore.api.{EventInStream, EventStore}
import com.timgroup.eventsubscription.healthcheck.{ChaserHealth, EventSubscriptionStatus, SubscriptionListenerAdapter}
import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.tucker.info.{Component, Health}

trait EventProcessorListener {
  def eventProcessingFailed(version: Long, e: Exception): Unit

  def eventProcessed(version: Long): Unit

  def eventDeserializationFailed(version: Long, e: Exception): Unit

  def eventDeserialized(version: Long): Unit
}

class EventSubscription[T](
            name: String,
            eventstore: EventStore,
            deserializer: EventInStream => T,
            handlers: List[EventHandler[T]],
            clock: Clock = SystemClock,
            bufferSize: Int = 1024,
            runFrequency: Long = 1000,
            fromVersion: Long = 0) {
  private val chaserHealth = new ChaserHealth(name, clock)
  private val subscriptionStatus = new EventSubscriptionStatus(name, clock)

  private val processorListener = new SubscriptionListenerAdapter(subscriptionStatus)
  private val chaserListener = new BroadcastingChaserListener(chaserHealth, processorListener)

  val statusComponents: List[Component] = List(subscriptionStatus, chaserHealth)
  val health: Health = subscriptionStatus

  private val executor = Executors.newScheduledThreadPool(2, new ThreadFactory {
    private val count = new AtomicInteger()

    override def newThread(r: Runnable) = {
      val thread = new Thread(r, "EventSubscriptionRunner-" + name + "-" + count.getAndIncrement)
      thread.setDaemon(true)
      thread
    }
  })

  private val eventHandler = new BroadcastingEventHandler[T](handlers)

  private val eventHandlerExecutor = Executors.newCachedThreadPool()
  private val disruptor = new Disruptor[EventContainer[T]](new EventContainerFactory[T], bufferSize, eventHandlerExecutor)

  private val deserializeWorker = new WorkHandler[EventContainer[T]] {
    override def onEvent(eventContainer: EventContainer[T]): Unit = {
      try {
        eventContainer.deserializedEvent = deserializer(eventContainer.event)
        processorListener.eventDeserialized(eventContainer.event.version)
      } catch {
        case e: Exception => processorListener.eventDeserializationFailed(eventContainer.event.version, e)
      }
    }
  }

  private val invokeEventHandlers = new com.lmax.disruptor.EventHandler[EventContainer[T]] {
    override def onEvent(eventContainer: EventContainer[T], sequence: Long, endOfBatch: Boolean): Unit = {
      try {
        eventHandler.apply(eventContainer.event, eventContainer.deserializedEvent)
        processorListener.eventProcessed(eventContainer.event.version)
      } catch {
        case e: Exception => processorListener.eventProcessingFailed(eventContainer.event.version, e); throw e
      }
    }

  }

  disruptor.handleEventsWithWorkerPool(deserializeWorker, deserializeWorker, deserializeWorker)
           .then(invokeEventHandlers)

  def start() {
    val chaser = new EventStoreChaser(eventstore, fromVersion, evt => disruptor.publishEvent(new SetEventInStream(evt)), chaserListener)

    disruptor.start()

    executor.scheduleWithFixedDelay(chaser, 0, runFrequency, TimeUnit.MILLISECONDS)
  }

  def stop() {
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.SECONDS)
    disruptor.shutdown()
    eventHandlerExecutor.shutdown()
    eventHandlerExecutor.awaitTermination(1, TimeUnit.SECONDS)
  }
}

class EventContainer[T](var event: EventInStream, var deserializedEvent: T)

class EventContainerFactory[T] extends EventFactory[EventContainer[T]] {
  override def newInstance(): EventContainer[T] = new EventContainer(null, null.asInstanceOf[T])
}

class SetEventInStream[T](evt: EventInStream) extends EventTranslator[EventContainer[T]] {
  override def translateTo(eventContainer: EventContainer[T], sequence: Long): Unit = eventContainer.event = evt
}