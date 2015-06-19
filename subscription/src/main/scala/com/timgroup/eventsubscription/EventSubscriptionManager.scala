package com.timgroup.eventsubscription

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, Executors, ThreadFactory, TimeUnit}

import com.timgroup.eventstore.api.{EventInStream, EventStore}
import com.timgroup.eventsubscription.healthcheck.{SubscriptionListenerAdapter, ChaserHealth, EventSubscriptionStatus}
import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.tucker.info.{Component, Health}

trait EventProcessorListener {
  def eventProcessingFailed(version: Long, e: Exception): Unit

  def eventProcessed(version: Long)
}

class EventProcessor(eventHandler: EventHandler,
                     eventQueue: ArrayBlockingQueue[EventInStream],
                     listener: EventProcessorListener) extends Runnable {
  override def run(): Unit = {
    while (true) {
      val event = eventQueue.take()

      try {
        eventHandler.apply(event)
        listener.eventProcessed(event.version)
      } catch {
        case e: Exception => listener.eventProcessingFailed(event.version, e); throw e
      }
    }
  }
}

class EventSubscriptionManager(
            name: String,
            eventstore: EventStore,
            handlers: List[EventHandler],
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