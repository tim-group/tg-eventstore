package com.timgroup.eventsubscription

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, Executors, ThreadFactory, TimeUnit}

import com.timgroup.eventstore.api.{EventInStream, EventStore}
import com.timgroup.eventsubscription.healthcheck.{ChaserHealth, EventStreamVersionComponent, EventSubscriptionStatus}
import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.tucker.info.{Component, Health}

trait EventProcessorListener {
  def eventProcessingFailed(e: Exception): Unit

  def eventProcessed(version: Long)
}

class EventProcessor(eventHandler: EventHandler,
                     eventQueue: ArrayBlockingQueue[EventInStream],
                     listener: EventProcessorListener) extends Runnable {
  override def run(): Unit = {
    while (true) {
      try {
        val event = eventQueue.take()
        eventHandler.apply(event)
        listener.eventProcessed(event.version)
      } catch {
        case e: Exception => listener.eventProcessingFailed(e)
      }
    }
  }
}

class EventSubscriptionManager(
            name: String,
            eventstore: EventStore,
            handlers: List[EventHandler],
            chaserListener: ChaserListener,
            processorListener: EventProcessorListener,
            bufferSize: Int,
            runFrequency: Long,
            fromVersion: Long) {
  private val executor = Executors.newScheduledThreadPool(2, new ThreadFactory {
    private val count = new AtomicInteger()

    override def newThread(r: Runnable) = {
      val thread = new Thread(r, "EventSubscriptionRunner-" + name + "-" + count.getAndIncrement)
      thread.setDaemon(true)
      thread
    }
  })

  def start() {
    val eventHandler = new BroadcastingEventHandler(handlers)

    val eventQueue = new ArrayBlockingQueue[EventInStream](bufferSize)

    val chaser = new EventStoreChaser(eventstore, fromVersion, eventQueue.put, chaserListener)

    val eventProcessor = new EventProcessor(eventHandler, eventQueue, processorListener)

    executor.scheduleWithFixedDelay(chaser, 0, runFrequency, TimeUnit.MILLISECONDS)
    executor.submit(eventProcessor)
  }

  def stop() {
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.SECONDS)
  }
}

object EventSubscriptionManager {
  case class SubscriptionSetup(health: Health, components: List[Component], subscriptionManager: EventSubscriptionManager)

  def apply(name: String,
            eventStore: EventStore,
            handlers: List[EventHandler],
            clock: Clock = SystemClock,
            bufferSize: Int = 50000,
            frequency: Long = 1000,
            fromVersion: Long = 0) = {

    val chaserHealth = new ChaserHealth(name, clock)
    val subscriptionStatus = new EventSubscriptionStatus(name)
    val versionComponent = new EventStreamVersionComponent(name)

    val manager =
      new EventSubscriptionManager(name, eventStore, handlers ++ List(versionComponent), new BroadcastingChaserListener(chaserHealth, subscriptionStatus),
        subscriptionStatus, bufferSize, frequency, fromVersion)

    SubscriptionSetup(subscriptionStatus, List(subscriptionStatus, chaserHealth, versionComponent), manager)
  }
}
