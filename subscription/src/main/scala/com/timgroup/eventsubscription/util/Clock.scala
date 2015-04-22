package com.timgroup.eventsubscription.util

import org.joda.time.{DateTimeZone, DateTime}

trait Clock {
  def now(): DateTime
}

object SystemClock extends Clock {
  override def now(): DateTime = DateTime.now(DateTimeZone.UTC)
}
