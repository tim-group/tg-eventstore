package com.timgroup.mysqleventstore.sql

import java.sql.Connection

import com.timgroup.mysqleventstore.{EventInStream, EventPage}

class BackfillStitchingEventFetcher(backfill: EventFetcher,
                                    live: EventFetcher) extends EventFetcher {
  override def fetchEventsFromDB(connection: Connection, version: Long, batchSize: Option[Int]): Seq[EventInStream] = {
    val backfillEvents = backfill.fetchEventsFromDB(connection, version, batchSize)

    if (batchSize.map(backfillEvents.size >= _).getOrElse(false)) {
      backfillEvents
    } else {
      val liveEvents = live.fetchEventsFromDB(connection, version, batchSize)

      val allEvents = (backfillEvents ++ liveEvents)

      allEvents.take(batchSize.getOrElse(allEvents.size))
    }
  }



}

class BackfillStitchingHeadVersionFetcher(backfillHeadVersion: HeadVersionFetcher,
                                          liveHeadVersion: HeadVersionFetcher) extends HeadVersionFetcher {
  override def fetchCurrentVersion(connection: Connection): Long = {
    val liveVersion: Long = liveHeadVersion.fetchCurrentVersion(connection)

    if (liveVersion > 0) {
      liveVersion
    } else {
      backfillHeadVersion.fetchCurrentVersion(connection)
    }
  }
}
