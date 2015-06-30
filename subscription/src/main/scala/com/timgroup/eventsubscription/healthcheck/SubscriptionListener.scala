package com.timgroup.eventsubscription.healthcheck

import com.timgroup.eventsubscription.{EventProcessorListener, ChaserListener}


trait SubscriptionListener {
  def caughtUpAt(version: Long): Unit

  def staleAtVersion(version: Option[Long]): Unit

  def terminated(version: Long, e: Exception): Unit
}

class SubscriptionListenerAdapter(listeners: SubscriptionListener*) extends ChaserListener with EventProcessorListener {
  @volatile private var latestFetchedVersion: Option[Long] = None
  @volatile private var latestProcessedVersion: Option[Long] = None

  override def transientFailure(e: Exception): Unit = {}

  override def chaserReceived(version: Long): Unit = {
    latestFetchedVersion = None
    checkStaleness()
  }

  override def chaserUpToDate(version: Long): Unit = {
    latestFetchedVersion = Some(version)
    checkStaleness()
  }

  override def eventProcessingFailed(version: Long, e: Exception): Unit = {
    listeners.foreach(_.terminated(version, e))
  }

  override def eventProcessed(version: Long): Unit = {
    latestProcessedVersion = Some(version)
    checkStaleness()
  }

  override def eventDeserializationFailed(version: Long, e: Exception): Unit = {
    listeners.foreach(_.terminated(version, e))
  }

  override def eventDeserialized(version: Long): Unit = {}

  private def checkStaleness(): Unit = {
    (latestFetchedVersion, latestProcessedVersion) match {
      case (Some(fetchedVersion), Some(processedVersion)) if processedVersion >= fetchedVersion => listeners.foreach(_.caughtUpAt(processedVersion))
      case (_, Some(processedVersion)) => listeners.foreach(_.staleAtVersion(Some(processedVersion)))
      case _ => listeners.foreach(_.staleAtVersion(None))
    }
  }
}