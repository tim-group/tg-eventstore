package com.timgroup.eventstore.api

import org.joda.time.{DateTime, DateTimeZone}

trait Clock {
  def now(): DateTime
}

object SystemClock extends Clock {
  override def now(): DateTime = DateTime.now(DateTimeZone.UTC)
}
