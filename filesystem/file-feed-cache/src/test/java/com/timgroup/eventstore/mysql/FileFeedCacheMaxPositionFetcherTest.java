package com.timgroup.eventstore.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.MatcherAssert;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;


import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.CODEC;
import static com.timgroup.eventstore.mysql.FileFeedCacheEventSourceTest.archivedEvent;
import static org.hamcrest.CoreMatchers.equalTo;

public class FileFeedCacheMaxPositionFetcherTest {
    private static final String EVENT_STORE_ID = "anEventStoreId";

    private Instant timestamp = Instant.parse("2018-11-20T01:00:00Z");

    @Test public void no_max_position_fetched_when_archive_does_not_exist() {
        FileFeedCacheEventSourceTest.FakeReadableFeedStorage storage = new FileFeedCacheEventSourceTest.FakeReadableFeedStorage(ImmutableMap.of());

        final FileFeedCacheMaxPositionFetcher positionFetcher = new FileFeedCacheMaxPositionFetcher(storage, new ArchiveKeyFormat(EVENT_STORE_ID));

        MatcherAssert.assertThat(positionFetcher.maxPosition(), equalTo(Optional.empty()));
    }

    @Test public void fetches_max_position_in_the_archive() {
        FileFeedCacheEventSourceTest.FakeReadableFeedStorage storage = new FileFeedCacheEventSourceTest.FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0002.gz", ImmutableList.of(archivedEvent(1), archivedEvent(2)),
                EVENT_STORE_ID + "/0004.gz", ImmutableList.of(archivedEvent(3), archivedEvent(4)),
                EVENT_STORE_ID + "/0005.gz", ImmutableList.of(archivedEvent(5), archivedEvent(6))
        ));

        final FileFeedCacheMaxPositionFetcher positionFetcher = new FileFeedCacheMaxPositionFetcher(storage, new ArchiveKeyFormat(EVENT_STORE_ID));

        MatcherAssert.assertThat(positionFetcher.maxPosition().get(), equalTo(CODEC.deserializePosition("5")));
    }

    @Test public void no_max_position_before_a_given_time_fetched_when_archive_does_not_exist() {
        FileFeedCacheEventSourceTest.FakeReadableFeedStorage storage = new FileFeedCacheEventSourceTest.FakeReadableFeedStorage(ImmutableMap.of());

        final FileFeedCacheMaxPositionFetcher positionFetcher = new FileFeedCacheMaxPositionFetcher(storage, new ArchiveKeyFormat(EVENT_STORE_ID));

        MatcherAssert.assertThat(positionFetcher.maxPositionBefore(java.time.Instant.now()), equalTo(Optional.empty()));
    }

    @Test public void fetches_max_position_before_a_given_time() {
        FileFeedCacheEventSourceTest.FakeReadableFeedStorage storage = new FileFeedCacheEventSourceTest.FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0002.gz", ImmutableList.of(archivedEvent(1), archivedEvent(2)),
                EVENT_STORE_ID + "/0004.gz", ImmutableList.of(archivedEvent(3), archivedEvent(4)),
                EVENT_STORE_ID + "/0005.gz", ImmutableList.of(archivedEvent(5), archivedEvent(6))
        ));

        storage.setArrivalTime(EVENT_STORE_ID + "/0002.gz", timestamp);
        storage.setArrivalTime(EVENT_STORE_ID + "/0004.gz", timestamp.plus(Duration.standardDays(10)));
        storage.setArrivalTime(EVENT_STORE_ID + "/0005.gz", Instant.now());


        final FileFeedCacheMaxPositionFetcher positionFetcher = new FileFeedCacheMaxPositionFetcher(storage, new ArchiveKeyFormat(EVENT_STORE_ID));
        final Optional<BasicMysqlEventStorePosition> position = positionFetcher.maxPositionBefore(java.time.Instant.now().minus(5, ChronoUnit.DAYS));
        MatcherAssert.assertThat(position.get(), equalTo(CODEC.deserializePosition("4")));
    }



}