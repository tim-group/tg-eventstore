package com.timgroup.eventsubscription

trait EventProcessorListener {
  def eventProcessingFailed(version: Long, e: Exception): Unit

  def eventProcessed(version: Long): Unit

  def eventDeserializationFailed(version: Long, e: Exception): Unit

  def eventDeserialized(version: Long): Unit
}
