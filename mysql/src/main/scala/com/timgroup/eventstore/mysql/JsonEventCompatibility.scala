package com.timgroup.eventstore.mysql

import java.io.IOException

import scala.collection.JavaConversions._

import com.timgroup.eventstore.api._

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
object JsonEventCompatibility extends CompatibilityPredicate {
  private val objectMapper = new ObjectMapper
  private val nodeReader = objectMapper.readerFor(classOf[ObjectNode])

  override def test(version:Long, currentEvent: EventData, newEvent: EventData): Boolean = {
    if (currentEvent.eventType != newEvent.eventType) return false
    if (currentEvent.body == newEvent.body) return true
    val currentDocument = parse(currentEvent, "Invalid JSON produced for event stream")
    val newDocument = parse(newEvent, "Invalid JSON retrieved from event stream")
    compareObjects(currentDocument, newDocument, new Context(currentEvent.eventType, version, currentDocument, newDocument))
    true
  }

  def compareNodes(currentNode: JsonNode, newNode: JsonNode, ctx: Context): Unit = {
    (currentNode, newNode) match {
      case(currentObject : ObjectNode, newObject : ObjectNode) => compareObjects(currentObject, newObject, ctx)
      case(currentArray : ArrayNode, newArray : ArrayNode) => compareArrays(currentArray, newArray, ctx)
      case(_, _) if (currentNode != newNode) => throw ctx.idempotentWriteFailure("Event documents do not match")
      case _ => Unit
    }
  }

  def compareObjects(currentObject: ObjectNode, newObject: ObjectNode, ctx: Context): Unit =
    new MergeTraversable(sortedFields(currentObject), sortedFields(newObject)).foreach {
      case Both(fieldName, currentNode, newNode) => compareNodes(currentNode, newNode, ctx.appendField(fieldName))
      case Left(fieldName, currentNode) =>
        if (!currentNode.isNull) throw ctx.appendField(fieldName).idempotentWriteFailure("Element in current version, but not new version")
      case Right(fieldName, newNode) => Unit
    }

  class MergeTraversable[K : Ordering, V](val left: Seq[(K, V)], val right: Seq[(K, V)]) extends Traversable[Merged[K, V]] {
    val ordering = implicitly[Ordering[K]]
    def foreach[U](f: Merged[K, V] => U) = {
      var leftPtr = left
      var rightPtr = right
      while (!leftPtr.isEmpty && !rightPtr.isEmpty) {
        val n = ordering.compare(leftPtr.head._1, rightPtr.head._1)
        if (n < 0) {
          f(Left(leftPtr.head._1, leftPtr.head._2))
          leftPtr = leftPtr.tail
        }
        else if (n > 0) {
          f(Right(rightPtr.head._1, rightPtr.head._2))
          rightPtr = rightPtr.tail
        }
        else {
          f(Both(leftPtr.head._1, leftPtr.head._2, rightPtr.head._2))
          leftPtr = leftPtr.tail
          rightPtr = rightPtr.tail
        }
      }
      leftPtr.foreach(l => f(Left(l._1, l._2)))
      rightPtr.foreach(r => f(Right(r._1, r._2)))
    }
  }

  sealed trait Merged[K, V]
  case class Both[K, V](key: K, left: V, right: V) extends Merged[K, V]
  case class Left[K, V](key: K, value: V) extends Merged[K, V]
  case class Right[K, V](key: K, value: V) extends Merged[K, V]

  def compareArrays(currentArray: ArrayNode, newArray: ArrayNode, ctx: Context): Unit = {
    if (currentArray.size != newArray.size()) throw ctx.idempotentWriteFailure("Array lengths do not match")
    (0 to (currentArray.size - 1)).foreach{ index => compareNodes(currentArray.get(index), newArray.get(index), ctx.appendIndex(index) ) }
  }

  def sortedFields(obj: ObjectNode) = obj.fieldNames.toList.sorted.map(name => (name, obj.get(name)))

  def parse(input: EventData, errorMessage: String): ObjectNode = {
    try {
      nodeReader.readValue[ObjectNode](input.body.data)
    } catch {
      case e: IOException => throw new IllegalStateException(errorMessage, e)
    }
  }

  case class Context(eventType: String, version: Long, currentDocument: JsonNode, newDocument: JsonNode, path: List[String] = List()) {
    def appendField(fieldName: String): Context = this.copy(path = fieldName :: path)
    def appendIndex(index: Int): Context = this.copy(path = "%s[%d]".format(path.head, index) :: path.tail)
    def idempotentWriteFailure(message: String): IdempotentWriteFailure = new IdempotentWriteFailure("%s for version %d (%s) at %s\n".format(message, version, eventType, path.reverse.mkString("."))
                                                                                                     + "current: " + currentDocument
                                                                                                     + "    new: " + newDocument)
  }
}
