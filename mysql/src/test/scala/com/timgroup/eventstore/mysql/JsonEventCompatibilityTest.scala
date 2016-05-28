package com.timgroup.eventstore.mysql

import com.timgroup.eventstore.api._

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.databind._

import org.scalatest._
import org.scalatest.OptionValues._

class JsonEventCompatibilityTest extends FlatSpec with Matchers {
  "compatibility predicate" should "regard identical events as compatible" in {
    errorComparing(mkevent("TestEvent", "{ a: 1 }"), mkevent("TestEvent", "{ a: 1 }")) should not be ('defined)
  }

  it should "regard events with different types as incompatible" in {
    errorComparing(mkevent("TestEventA", "{ a: 1 }"), mkevent("TestEventB", "{ a: 1 }")) should be ('defined)
  }

  it should "regard reordering fields as compatible" in {
    errorComparing(mkevent("TestEvent", "{ a: 1, b: 2 }"), mkevent("TestEvent", "{ b: 2, a: 1 }")) should not be ('defined)
  }

  it should "regard changing a string value as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: 'x' }"), mkevent("TestEvent", "{ a: 'y' }")) should be ('defined)
  }

  it should "regard changing a string value to a number value as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: '1' }"), mkevent("TestEvent", "{ a: 1 }")) should be ('defined)
  }

  it should "regard changing null to a value as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: null }"), mkevent("TestEvent", "{ a: 'x' }")) should be ('defined)
  }

  it should "regard changing a value to null as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: 'x' }"), mkevent("TestEvent", "{ a: null }")) should be ('defined)
  }

  it should "regard adding a new field as compatible" in {
    errorComparing(mkevent("TestEvent", "{ a: 'x' }"), mkevent("TestEvent", "{ a: 'x', b: 'y' }")) should not be ('defined)
  }

  it should "regard removing a field as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: 'x', b: 'y' }"), mkevent("TestEvent", "{ a: 'x' }")).value should include ("Element in current version, but not new version")
  }

  it should "regard adding an array value as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: ['x'] }"), mkevent("TestEvent", "{ a: ['x', 'y'] }")) should be ('defined)
  }

  it should "regard removing an array value as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: ['x', 'y'] }"), mkevent("TestEvent", "{ a: ['x'] }")) should be ('defined)
  }

  it should "regard changing a string value nested inside an object as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: { b: 'x' } }"), mkevent("TestEvent", "{ a: { b: 'y' } }")) should be ('defined)
  }

  it should "regard changing a string value nested inside an array as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: [ 'x' ] }"), mkevent("TestEvent", "{ a: [ 'y' ] }")) should be ('defined)
  }

  it should "regard reordering the values of an array as incompatible" in {
    errorComparing(mkevent("TestEvent", "{ a: [ 'x', 'y' ] }"), mkevent("TestEvent", "{ a: [ 'y', 'x' ] }")) should be ('defined)
  }

  "error messages" should "include the current and new documents" in {
    errorComparing(mkevent("TestEvent", "{ a: 'x' }"), mkevent("TestEvent", "{ a: 'y' }")).value should include ("""current: {"a":"x"}""")
    errorComparing(mkevent("TestEvent", "{ a: 'x' }"), mkevent("TestEvent", "{ a: 'y' }")).value should include ("""new: {"a":"y"}""")
  }

  it should "indicate nesting within an object" in {
    errorComparing(mkevent("TestEvent", "{ a: {b: 'x'} }"), mkevent("TestEvent", "{ a: {b: 'y'} }")).value should include ("""a.b""")
  }

  it should "indicate nesting within an array" in {
    errorComparing(mkevent("TestEvent", "{ a: ['x'] }"), mkevent("TestEvent", "{ a: ['y'] }")).value should include ("""a[0]""")
  }

  it should "indicate the version number" in {
    errorComparing(mkevent("TestEvent", "{ a: 'x' }"), mkevent("TestEvent", "{ a: 'y' }"), 12345).value should include ("""version 12345""")
  }

  private def mkevent(eventType: String, jsonString: String) = {
    val objectMapper = new ObjectMapper
    val jsonNode = objectMapper.reader
                                 .`with`(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
                                 .`with`(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                                 .`with`(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                                 .readTree(jsonString)
    val jsonBytes = objectMapper.writeValueAsBytes(jsonNode)
    EventData(eventType, jsonBytes)
  }

  private def errorComparing(currentEvent: EventData, newEvent: EventData, version: Long = 1L): Option[String] = {
    try {
      JsonEventCompatibility.test(version, currentEvent, newEvent) match {
        case true => None
        case false => Some("")
      }
    } catch {
      case e: IdempotentWriteFailure => return Some(e.getMessage)
    }
  }
}
