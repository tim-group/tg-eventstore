package com.timgroup.eventstore.vector

import com.timgroup.eventstore.api.EventInStream

object EffectiveEventOrdering extends Ordering[EventInStream] {
  override def compare(x: EventInStream, y: EventInStream): Int = x.effectiveTimestamp.getMillis.compareTo(y.effectiveTimestamp.getMillis)
}

class CombinedIterator[T](_iterators: List[Iterator[T]])(implicit ordering: Ordering[T]) extends Iterator[T] {
  var iterators = _iterators.map(_.buffered)

  def hasNext: Boolean = {
    iterators = iterators.filter(_.hasNext)
    iterators.foldLeft(false)(_ || _.hasNext)
  }

  def next(): T = {
    iterators
      .filter(_.hasNext)
      .sortBy(_.head)
      .head
      .next()
  }
}