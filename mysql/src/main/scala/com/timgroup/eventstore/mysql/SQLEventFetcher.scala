package com.timgroup.eventstore.mysql

import java.sql.Connection

import com.timgroup.eventstore.api.{EventInStream, EventData}
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.control.Exception.allCatch

class SQLEventFetcher(tableName: String) {
  def fetchEventsFromDB(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): Seq[EventInStream] = {
    val statement = connection.prepareStatement("select effective_timestamp, eventType, body, version from  %s where version > ? %s".format(tableName, batchSize.map("limit " + _).getOrElse("")))
    statement.setLong(1, version)

    val results = statement.executeQuery()

    try {
      val eventsIterator = new Iterator[EventInStream] {
        override def hasNext = results.next()

        override def next() = EventInStream(
          new DateTime(results.getTimestamp("effective_timestamp"),DateTimeZone.UTC),
          EventData(
            eventType = results.getString("eventType"),
            body = results.getBytes("body")),
          results.getLong("version")
        )
      }

      eventsIterator.toList
    } finally {
      allCatch opt { statement.close() }
      allCatch opt { results.close() }
    }
  }
}