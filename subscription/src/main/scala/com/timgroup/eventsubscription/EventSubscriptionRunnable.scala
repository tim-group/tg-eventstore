package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.{EventInStream, EventStore}

class EventSubscriptionRunnable(eventstore: EventStore,
                                handler: EventHandler,
                                listener: EventSubscriptionListener = NoopSubscriptionListener,
                                batchSize: Option[Int] = None) extends Runnable {
  private val eventStream = eventstore.fromAll(0)

  private var initialReplayDone = false

  private def initialReplay(): Unit = {
    listener.eventSubscriptionStarted()
    eventStream.foreach(applyToHandler)
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
