package com.timgroup.eventstore.api

import java.util

class EventStreamJavaAdapter(delegate: java.util.stream.Stream[EventInStream]) extends EventStream {
  private val underlying: util.Iterator[EventInStream] = delegate.iterator()

  override def hasNext: Boolean = underlying.hasNext

  override def next(): EventInStream = underlying.next()
}
