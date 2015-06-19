package com.timgroup.eventsubscription

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.{EventFactory, EventTranslator}
import com.timgroup.eventstore.api.{EventInStream, EventStore}
import com.timgroup.eventsubscription.healthcheck.{ChaserHealth, EventSubscriptionStatus, SubscriptionListenerAdapter}
import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.tucker.info.{Component, Health}

trait EventProcessorListener {
  def eventProcessingFailed(version: Long, e: Exception): Unit

  def eventProcessed(version: Long)
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
  private val disruptor = new Disruptor[EventContainer](EventContainer, bufferSize, eventHandlerExecutor)

  disruptor.handleEventsWith(new com.lmax.disruptor.EventHandler[EventContainer] {
    override def onEvent(eventContainer: EventContainer, sequence: Long, endOfBatch: Boolean): Unit = {
      try {
        eventHandler.apply(eventContainer.event, deserializer(eventContainer.event))
        processorListener.eventProcessed(eventContainer.event.version)
      } catch {
        case e: Exception => processorListener.eventProcessingFailed(eventContainer.event.version, e); throw e
      }
    }
  })

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

class EventContainer(var event: EventInStream)

object EventContainer extends EventFactory[EventContainer] {
  override def newInstance(): EventContainer = new EventContainer(null)
}

class SetEventInStream(evt: EventInStream) extends EventTranslator[EventContainer] {
  override def translateTo(eventContainer: EventContainer, sequence: Long): Unit = eventContainer.event = evt
}