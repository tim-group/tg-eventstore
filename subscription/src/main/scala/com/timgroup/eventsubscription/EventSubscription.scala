package com.timgroup.eventsubscription

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.function.Consumer

import com.lmax.disruptor.dsl.{Disruptor, ProducerType}
import com.lmax.disruptor.{BlockingWaitStrategy, EventFactory, EventTranslator, WorkHandler}
import com.timgroup.eventstore.api.{Clock, EventInStream, EventStore, SystemClock}
import com.timgroup.eventsubscription.healthcheck.{ChaserHealth, EventSubscriptionStatus, SubscriptionListener, SubscriptionListenerAdapter}
import com.timgroup.tucker.info.{Component, Health}

import scala.collection.JavaConversions._
import scala.collection.mutable


class EventSubscription[T](
            name: String,
            eventstore: EventStore,
            deserializer: Deserializer[T],
            handlers: java.util.List[EventHandler[T]],
            clock: Clock = SystemClock,
            bufferSize: java.lang.Integer,
            runFrequency: java.lang.Long,
            fromVersion: java.lang.Long,
            maxInitialReplayDuration: java.lang.Integer,
            listeners: java.util.List[SubscriptionListener]) {

  private val chaserHealth = new ChaserHealth(name, clock)
  private val subscriptionStatus = new EventSubscriptionStatus(name, clock, maxInitialReplayDuration)

  import scala.collection.JavaConversions._
  private val subListeners: java.util.List[SubscriptionListener] = subscriptionStatus.asInstanceOf[SubscriptionListener] +: listeners
  private val processorListener = new SubscriptionListenerAdapter(fromVersion, subListeners)
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

  private val disruptor = new Disruptor[EventContainer[T]](new EventContainerFactory[T], bufferSize, eventHandlerExecutor, ProducerType.SINGLE, new BlockingWaitStrategy())

  private val deserializeWorker = new WorkHandler[EventContainer[T]] {
    override def onEvent(eventContainer: EventContainer[T]): Unit = {
      try {
        eventContainer.deserializedEvent = deserializer.deserialize(eventContainer.event)
        processorListener.eventDeserialized(eventContainer.event.version)
      } catch {
        case e: Exception => {
          processorListener.eventDeserializationFailed(eventContainer.event.version, e)
          throw e
        }
      }
    }
  }

  private val invokeEventHandlers = new com.lmax.disruptor.EventHandler[EventContainer[T]] {
    override def onEvent(eventContainer: EventContainer[T], sequence: Long, endOfBatch: Boolean): Unit = {
      try {
        eventHandler.apply(eventContainer.event, eventContainer.deserializedEvent, endOfBatch)
        processorListener.eventProcessed(eventContainer.event.version)
      } catch {
        case e: Exception => processorListener.eventProcessingFailed(eventContainer.event.version, e); throw e
      }
    }

  }

  disruptor.handleEventsWithWorkerPool(deserializeWorker, deserializeWorker).then(invokeEventHandlers)

  def start() {
    val chaser = new EventStoreChaser(eventstore, fromVersion, new Consumer[EventInStream] {
      override def accept(evt: EventInStream): Unit = disruptor.publishEvent(new SetEventInStream(evt))
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

class EventContainer[T](var event: EventInStream, var deserializedEvent: T)

class EventContainerFactory[T] extends EventFactory[EventContainer[T]] {
  override def newInstance(): EventContainer[T] = new EventContainer(null, null.asInstanceOf[T])
}

class SetEventInStream[T](evt: EventInStream) extends EventTranslator[EventContainer[T]] {
  override def translateTo(eventContainer: EventContainer[T], sequence: Long): Unit = eventContainer.event = evt
}
