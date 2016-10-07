package com.timgroup.eventsubscription.healthcheck

import java.util.OptionalLong

import com.timgroup.eventsubscription.{ChaserListener, EventProcessorListener}

class SubscriptionListenerAdapter(fromVersion: Long, listeners: SubscriptionListener*) extends ChaserListener with EventProcessorListener {
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
      case (Some(`fromVersion`), None) => listeners.foreach(_.caughtUpAt(fromVersion))
      case (_, Some(processedVersion)) => listeners.foreach(_.staleAtVersion(OptionalLong.of(processedVersion)))
      case _ => listeners.foreach(_.staleAtVersion(OptionalLong.empty()))
    }
  }
}