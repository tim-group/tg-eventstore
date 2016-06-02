package com.timgroup.eventstore.api

import java.util

class EventStreamJavaAdapter(delegate: java.util.stream.Stream[EventInStream]) extends EventStream {
  private val iterator: util.Iterator[EventInStream] = delegate.iterator()
  override def hasNext: Boolean = iterator.hasNext

  override def next(): EventInStream = iterator.next()
}
