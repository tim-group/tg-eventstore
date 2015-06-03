package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.{EventProcessorListener, ChaserListener}


trait SubscriptionListener {
  def caughtUpAt(version: Long): Unit

  def staleAtVersion(version: Option[Long]): Unit

  def terminated(e: Exception): Unit
}

class SubscriptionListenerAdapter(listeners: SubscriptionListener*) extends ChaserListener with EventProcessorListener {
  @volatile private var liveVersion: Option[Long] = None
  @volatile private var processorVersion: Option[Long] = None

  override def transientFailure(e: Exception): Unit = {}

  override def chaserReceived(version: Long): Unit = {
    liveVersion = None
    checkStaleness()
  }

  override def chaserUpToDate(version: Long): Unit = {
    liveVersion = Some(version)
    checkStaleness()
  }

  override def eventProcessingFailed(e: Exception): Unit = {
    listeners.foreach(_.terminated(e))
  }

  override def eventProcessed(version: Long): Unit = {
    processorVersion = Some(version)
    checkStaleness()
  }

  private def checkStaleness(): Unit = {
    (liveVersion, processorVersion) match {
      case (Some(live), Some(processed)) if live <= processed => listeners.foreach(_.caughtUpAt(processed))
      case (_, Some(processed)) => listeners.foreach(_.staleAtVersion(Some(processed)))
      case _ => listeners.foreach(_.staleAtVersion(None))
    }
  }
}