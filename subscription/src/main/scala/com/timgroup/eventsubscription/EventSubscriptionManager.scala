package com.timgroup.eventsubscription

import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import com.timgroup.eventstore.api.EventStore
import com.timgroup.eventsubscription.healthcheck.{EventStoreSubscriptionHealth, EventStreamCatchupHealth}
import com.timgroup.eventsubscription.util.{Clock, SystemClock}
import com.timgroup.tucker.info.{Component, Health}
import org.slf4j.LoggerFactory

class EventSubscriptionManager(
            name: String,
            eventstore: EventStore,
            handlers: List[EventHandler],
            listener: EventSubscriptionListener,
            batchSize: Option[Int] = Some(100000)) {
  private val executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    override def newThread(r: Runnable) = {
      val thread = new Thread(r, "EventSubscriptionRunner-" + name)
      thread.setDaemon(true)
      thread
    }
  })

  def start() {
    val runnable = new EventSubscriptionRunnable(
                              eventstore,
                              new BroadcastingEventHandler(handlers),
                              listener,
                              batchSize)

    executor.scheduleAtFixedRate(errorHandling(runnable), 0, 1000, TimeUnit.MILLISECONDS)
  }

  def stop() {
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.SECONDS)
  }

  private def errorHandling(runnable: Runnable) = new Runnable {
    override def run(): Unit = {
      try {
        runnable.run()
      } catch {
        case t: Throwable => {
          LoggerFactory.getLogger(this.getClass).warn("Runnable failed", t)
          throw t
        }
      }
      listener.cycleCompleted()
    }
  }
}

object EventSubscriptionManager {
  case class SubscriptionSetup(health: Health, components: List[Component], subscriptionManager: EventSubscriptionManager)

  def apply(name: String,
            eventStore: EventStore,
            handlers: List[EventHandler],
            clock: Clock = SystemClock,
            batchSize: Option[Int] = Some(10000),
            listener: EventSubscriptionListener = NoopSubscriptionListener) = {
    val subscriptionHealth = new EventStoreSubscriptionHealth(name, clock)
    val catchupHealth = new EventStreamCatchupHealth(name, clock)

    val manager = new EventSubscriptionManager(name, eventStore, handlers ++ List(catchupHealth), new BroadcastingListener(subscriptionHealth, listener), batchSize)

    SubscriptionSetup(catchupHealth, List(catchupHealth, subscriptionHealth), manager)
  }
}
