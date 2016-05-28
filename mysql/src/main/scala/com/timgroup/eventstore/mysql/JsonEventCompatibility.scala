package com.timgroup.eventstore.mysql

import java.io.IOException

import scala.collection.JavaConversions._

import com.timgroup.eventstore.api._

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

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

  def compareObjects(currentObject: ObjectNode, newObject: ObjectNode, ctx: Context): Unit = {
    merge(sortedFields(newObject).iterator.buffered, sortedFields(currentObject).iterator.buffered,
                                    (fieldName: String) => compareNodes(currentObject.get(fieldName), newObject.get(fieldName), ctx.appendField(fieldName)),
                                    (newFieldName: String) => Unit,
                                    (currentFieldName: String) => {
                                      val currentNode = currentObject.get(currentFieldName)
                                      if (!currentNode.isNull)
                                        throw ctx.appendField(currentFieldName).idempotentWriteFailure("Element in current version, but not new version")
                                    })
  }

  def merge[T: Ordering](left: BufferedIterator[T], right: BufferedIterator[T], emitEqual: (T) => Unit, emitLeftOnly: (T) => Unit, emitRightOnly: (T) => Unit): Unit = {
    val ordering = implicitly[Ordering[T]]
    while (left.hasNext && right.hasNext) {
      if (ordering.equiv(left.head, right.head)) {
        val value = left.next
        right.next
        emitEqual(value)
      }
      else if (ordering.lt(left.head, right.head)) {
        emitLeftOnly(left.next)
      }
      else {
        emitRightOnly(right.next)
      }
    }
    while (left.hasNext) emitLeftOnly(left.next)
    while (right.hasNext) emitRightOnly(right.next)
  }

  def compareArrays(currentArray: ArrayNode, newArray: ArrayNode, ctx: Context): Unit = {
    if (currentArray.size != newArray.size()) throw ctx.idempotentWriteFailure("Array lengths do not match")
    (0 to (currentArray.size - 1)).foreach{ index => compareNodes(currentArray.get(index), newArray.get(index), ctx.appendIndex(index) ) }
  }

  def sortedFields(obj: ObjectNode) = obj.fieldNames.toList.sorted

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
