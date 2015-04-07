package com.timgroup.mysqleventstore.sql

import java.sql.Connection

import com.timgroup.mysqleventstore.{EventInStream, EventPage}

class BackfillStitchingEventFetcher(backfill: EventFetcher,
                                    backfillHeadVersion: SQLHeadVersionFetcher,
                                    live: EventFetcher,
                                    liveHeadVersion: SQLHeadVersionFetcher) extends EventFetcher {
  override def fetchEventsFromDB(connection: Connection, version: Long, batchSize: Option[Int]): EventPage = {
    val overallLastVersion = fetchCurrentVersion(connection)

    val backfillEvents = backfill.fetchEventsFromDB(connection, version, batchSize).events.toList

    if (batchSize.map(backfillEvents.size >= _).getOrElse(false)) {
      eventPage(backfillEvents, overallLastVersion)
    } else {
      val liveEvents = live.fetchEventsFromDB(connection, version, batchSize).events.toList

      val allEvents = (backfillEvents ++ liveEvents)

      eventPage(
        allEvents.take(batchSize.getOrElse(allEvents.size)),
        overallLastVersion
      )
    }
  }

  private def eventPage(events: Seq[EventInStream], lastVersion: Long) =
    EventPage(events.map(_.copy(lastVersion = lastVersion)))

  def fetchCurrentVersion(connection: Connection): Long = {
    val liveVersion: Long = liveHeadVersion.fetchCurrentVersion(connection)

    if (liveVersion > 0) {
      liveVersion
    } else {
      backfillHeadVersion.fetchCurrentVersion(connection)
    }
  }
}
