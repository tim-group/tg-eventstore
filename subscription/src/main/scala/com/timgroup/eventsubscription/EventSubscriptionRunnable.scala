package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.{EventPage, EventStore}
import com.timgroup.eventsubscription.healthcheck.EventStorePolled

import scala.annotation.tailrec

class EventSubscriptionRunnable(eventstore: EventStore,
                                handler: EventHandler,
                                listener: EventSubscriptionListener = NoopSubscriptionListener,
                                batchSize: Option[Int] = None) extends Runnable {
  private var currentVersion: Long = 0

  override def run() {
    catchUp()
  }

  @tailrec private def catchUp(): Unit = {
    val page = pollES()

    if (page.isDefined) {
      applyToHandler(page.get)

      page.get.lastOption.foreach { event =>
        currentVersion = event.version
      }

      if (!page.get.isLastPage.getOrElse(true)) {
        catchUp()
      }
    }
  }

  private def applyToHandler(evt: EventPage) = {
    try {
      handler.apply(evt)
    } catch {
      case e: Exception => {
        listener.eventHandlerFailure(e)
        throw e
      }
    }
  }

  private def pollES(): Option[EventPage] = try {
    val page = eventstore.fromAll(currentVersion, batchSize)
    listener.pollSucceeded(EventStorePolled(currentVersion, page.size))

    Some(page).filterNot(_.isEmpty)
  } catch {
    case e: Exception => {
      listener.pollFailed(e)
      None
    }
  }
}
