package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.{EventInStream, EventStore}

trait ChaserListener {
  def transientFailure(e: Exception): Unit

  def chaserReceived(version: Long): Unit

  def chaserUpToDate(version: Long): Unit
}

class BroadcastingChaserListener(listeners: ChaserListener*) extends ChaserListener {
  override def transientFailure(e: Exception): Unit = listeners.foreach(_.transientFailure(e))

  override def chaserReceived(version: Long): Unit = listeners.foreach(_.chaserReceived(version))

  override def chaserUpToDate(version: Long): Unit = listeners.foreach(_.chaserUpToDate(version))
}

class EventStoreChaser(eventStore: EventStore,
                       fromVersion: Long,
                       eventHandler: EventInStream => Unit,
                       listener: ChaserListener) extends Runnable {
  var lastVersion = fromVersion

  override def run(): Unit = {
    try {
      eventStore.fromAll(lastVersion, nextEvent => {
        listener.chaserReceived(nextEvent.version)
        lastVersion = nextEvent.version
        eventHandler(nextEvent)
      })
      listener.chaserUpToDate(lastVersion)
    } catch {
      case e: Exception => {
        listener.transientFailure(e)
      }
    }
  }
}


class EventStoreVectorChaser(
                            eventstores: List[EventStore],
                              eventStore: EventStore,
                       fromVersion: Long,
                       eventHandler: EventInStream => Unit,
                       listener: ChaserListener) extends Runnable {
  var lastVersion = fromVersion

  override def run(): Unit = {

    val iterator: Iterator[EventInStream] = null //eventstores.map { eventstore => }

    try {

      iterator.foreach {nextEvent =>
        listener.chaserReceived(nextEvent.version)
        lastVersion = nextEvent.version
        eventHandler(nextEvent)
      }

      eventStore.fromAll(lastVersion, nextEvent => {
        listener.chaserReceived(nextEvent.version)
        lastVersion = nextEvent.version
        eventHandler(nextEvent)
      })
      listener.chaserUpToDate(lastVersion)
    } catch {
      case e: Exception => {
        listener.transientFailure(e)
      }
    }
  }
}


