package com.timgroup.eventstore.mysql

import java.sql.Connection

import com.timgroup.eventstore.api.{EventInStream, EventData}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
class SQLEventFetcher(tableName: String) {
  def fetchEventsFromDB(connection: Connection, version: Long = 0, batchSize: Option[Int] = None): Seq[EventInStream] = {
    import ResourceManagement.withResource

    withResource(connection.prepareStatement("select effective_timestamp, eventType, body, version from  %s where version > ? %s".format(tableName, batchSize.map("limit " + _).getOrElse("")))) {
      statement =>
        statement.setLong(1, version)
        withResource(statement.executeQuery()) { results =>
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
        }
    }
  }
}