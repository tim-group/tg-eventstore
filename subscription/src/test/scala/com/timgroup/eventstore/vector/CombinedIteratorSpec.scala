package com.timgroup.eventstore.vector

import com.timgroup.eventstore.api.{EventData, EventInStream}
import org.joda.time.DateTime
import org.scalatest.{FunSpec, ShouldMatchers}

class CombinedIteratorSpec extends FunSpec with ShouldMatchers {
  it("all events in earlier iterators with same timestamp are returned before events in later iterators") {
    val timestamp = DateTime.now
    val e1 = EventInStream(timestamp, new EventData("e1", null), 0)
    val e2 = EventInStream(timestamp, new EventData("e2", null), 0)
    val e3 = EventInStream(timestamp, new EventData("e3", null), 0)
    val it1 = Iterator(e1, e2)
    val it2 = Iterator(e3)

    new CombinedIterator(List(it1, it2))(EffectiveEventOrdering).toList shouldBe List(e1, e2, e3)
  }
}

