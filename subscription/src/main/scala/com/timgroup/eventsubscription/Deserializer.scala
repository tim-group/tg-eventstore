package com.timgroup.eventsubscription

import com.timgroup.eventstore.api.EventInStream

trait Deserializer[T] {
  def deserialize(eventInStream: EventInStream): T
}
