package com.timgroup.eventsubscription

import java.util.concurrent.ExecutorService

import com.timgroup.eventstore.api.{EventInStream, EventStore}

class EventSubscriptionRunnable(eventstore: EventStore,
                                handler: EventHandler,
                                listener: EventSubscriptionListener = NoopSubscriptionListener,
                                bufferExecutor: ExecutorService,
                                bufferSize: Int,
                                fromVersion: Long) extends Runnable {
  private val eventStream = eventstore.fromAll(fromVersion)

  private var initialReplayDone = false

  private def initialReplay(): Unit = {
    listener.eventSubscriptionStarted()
    new BufferingIterator(eventStream, bufferExecutor, bufferSize).foreach(applyToHandler)
    listener.initialReplayCompleted()
  }

  override def run() {
    try {
      if (!initialReplayDone) {
        initialReplay()
        initialReplayDone = true
      }

      if (eventStream.hasNext) {
        listener.newEventsFound()
        eventStream.foreach(applyToHandler)
        listener.caughtUp()
      }

      listener.pollSucceeded()
    } catch {
      case e: EventHandlerFailed => throw e
      case e: Exception => listener.pollFailed(e)
    }

  }

  private def applyToHandler(evt: EventInStream) = {
    try {
      handler.apply(evt)
    } catch {
      case e: Exception => {
        listener.eventHandlerFailure(e)
        throw new EventHandlerFailed(e)
      }
    }
  }
}

class EventHandlerFailed(e: Exception) extends RuntimeException(e)

trait ChaserListener {
  def transientFailure(e: Exception): Unit

  def eventReceived(version: Long): Unit

  def chaserCaughtUp(version: Long): Unit
}

class EventStoreChaser(eventStore: EventStore,
                       fromVersion: Long,
                       eventHandler: EventInStream => Unit,
                       listener: ChaserListener) extends Runnable {
  var lastVersion = fromVersion

  override def run(): Unit = {
    try {
      eventStore.fromAll(lastVersion, nextEvent => {
        listener.eventReceived(nextEvent.version)
        lastVersion = nextEvent.version
        eventHandler(nextEvent)
      })
      listener.chaserCaughtUp(lastVersion)
    } catch {
      case e: Exception => {
        listener.transientFailure(e)
      }
    }
  }
}