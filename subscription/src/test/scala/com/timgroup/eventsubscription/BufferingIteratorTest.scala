package com.timgroup.eventsubscription

import java.util.concurrent.{TimeUnit, Executors}

import org.scalatest.{BeforeAndAfterEach, OneInstancePerTest, FunSpec, MustMatchers}
import org.scalatest.concurrent.Eventually._

class BufferingIteratorTest extends FunSpec with MustMatchers with OneInstancePerTest with BeforeAndAfterEach {
  val bufferExecutor = Executors.newSingleThreadExecutor()

  val brokenIterator = new Iterator[String] {
    override def hasNext: Boolean = true

    override def next(): String = throw new Exception("I am evil")
  }

  it("iterates over the underlying iterator") {
    val originalData = List("A", "B", "C", "D", "E")
    val underlying = originalData.iterator

    val buffered = new BufferingIterator(underlying, bufferExecutor, 4)

    buffered.toList must be(originalData)
  }

  it("propagates failures in buffering to the consumer") {
    val underlying = List("A", "B", "C", "D", "E").iterator ++ brokenIterator

    val buffered = new BufferingIterator(underlying, bufferExecutor, 4)

    val exception = intercept[Exception] {
      buffered.toList
    }

    exception.getMessage must be("I am evil")
  }

  it("reads ahead to the specified buffer size") {
    val underlying = new CountingIterator(List("A", "B", "C", "D", "E").iterator)

    val buffered = new BufferingIterator(underlying, bufferExecutor, 2)

    eventually {
      underlying.count must be(3)
    }

    buffered.next()
    buffered.next()

    eventually {
      underlying.count must be(5)
    }
  }

  override protected def afterEach(): Unit = {
    bufferExecutor.shutdown()
    bufferExecutor.awaitTermination(1, TimeUnit.SECONDS)
  }

  class CountingIterator(iterator: Iterator[String]) extends Iterator[String] {
    @volatile var count = 0

    override def hasNext: Boolean = iterator.hasNext

    override def next(): String = {
      count = count + 1
      iterator.next()
    }
  }
}