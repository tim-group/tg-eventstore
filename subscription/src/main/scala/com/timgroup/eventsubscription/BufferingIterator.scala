package com.timgroup.eventsubscription

import java.util.concurrent.{ExecutorService, ArrayBlockingQueue, Executor}

class BufferingIterator[T](underlying: Iterator[T], executor: ExecutorService, bufferSize: Int) extends Iterator[T] {
  case object Poison
  case class Failure(t: Throwable)

  private val queue = new ArrayBlockingQueue[Any](bufferSize)

  private var nextElem: Any = null

  executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        underlying.foreach(queue.put)
        queue.put(Poison)
      } catch {
        case t: Throwable => queue.put(Failure(t))
      }
    }
  })

  override def hasNext: Boolean = {
    maybeAdvance()
    nextElem != Poison
  }

  override def next(): T = {
    if (hasNext) {
      val thisElem = nextElem
      nextElem = null
      thisElem.asInstanceOf[T]
    } else {
       Iterator.empty.next()
    }
  }

  private def maybeAdvance(): Unit = {
    if (nextElem == null) {
      queue.take() match {
        case Failure(t) => throw t
        case elem => nextElem = elem
      }
    }
  }
}
