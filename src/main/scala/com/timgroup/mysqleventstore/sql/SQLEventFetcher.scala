package com.timgroup.mysqleventstore.sql

import java.sql.Connection

import com.timgroup.mysqleventstore.{EventData, EventInStream, EventPage}
import org.joda.time.{DateTime, DateTimeZone}

class SQLEventFetcher(tableName: String) extends EventFetcher {
  def fetchEventsFromDB(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): EventPage = {
    val statement = connection.prepareStatement("select effective_timestamp, eventType, body, version from  %s where version > ? %s".format(tableName, batchSize.map("limit " + _).getOrElse("")))
    statement.setLong(1, version)

    val last = fetchCurrentVersion(connection)

    val results = statement.executeQuery()

    try {
      val eventsIterator = new Iterator[EventInStream] {
        override def hasNext = results.next()

        override def next() = EventInStream(
          new DateTime(results.getTimestamp("effective_timestamp"),DateTimeZone.UTC),
          EventData(
            eventType = results.getString("eventType"),
            body = results.getBytes("body")),
          results.getLong("version"),
          last
        )
      }

      EventPage(eventsIterator.toList.toIterator)
    } finally {
      statement.close()
      results.close()
    }
  }

  def fetchCurrentVersion(connection: Connection): Long = {
    val statement = connection.prepareStatement("select max(version) from " + tableName)
    val results = statement.executeQuery()

    try {
      results.next()
      results.getLong(1)
    } finally {
      results.close()
      statement.close()
    }
  }
}
