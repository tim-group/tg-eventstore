package com.timgroup.eventstore.api

trait CompatibilityPredicate {
  def test(version: Long, currentEvent: EventData, newEvent: EventData): Boolean
}

object CompatibilityPredicate {
  object BytewiseEqual extends CompatibilityPredicate {
    override def test(version: Long, currentEvent: EventData, newEvent: EventData) =
      currentEvent.body == newEvent.body
  }
}
