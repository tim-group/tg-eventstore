package com.timgroup.eventsubscription

import java.util.function.Consumer
import java.util.stream.Stream

import com.timgroup.eventstore.api.{EventInStream, EventStore}

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
      val stream: Stream[EventInStream] = eventStore.streamingFromAll(lastVersion)

      try {
        stream.forEach(new Consumer[EventInStream] {
          override def accept(nextEvent: EventInStream): Unit = {
            listener.chaserReceived(nextEvent.version)
            lastVersion = nextEvent.version
            eventHandler(nextEvent)
          }
        })
      } finally {
        stream.close()
      }

      listener.chaserUpToDate(lastVersion)
    } catch {
      case e: Exception => {
        listener.transientFailure(e)
      }
    }
  }
}