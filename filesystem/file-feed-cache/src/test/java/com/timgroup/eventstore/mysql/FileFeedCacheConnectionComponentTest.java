package com.timgroup.eventstore.mysql;

import com.timgroup.tucker.info.Status;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;


public class FileFeedCacheConnectionComponentTest {

    @Test public void reports_ok_when_it_can_successfully_retrieve_max_postion_for_an_archive() {
        FileFeedCacheConnectionComponent underTest = new FileFeedCacheConnectionComponent("SomeEventStore", with(Optional.of(new BasicMysqlEventStorePosition(5L))));
        assertThat(underTest.getReport().getStatus(), CoreMatchers.equalTo(Status.OK));
        assertThat((String) underTest.getReport().getValue(),
                containsString("Successfully connected to File Feed EventStore Archive, max position=5"));
    }

    @Test public void reports_critical_when_it_can_connect_but_not_the_retrieve_max_postion_for_an_archive() {
        FileFeedCacheConnectionComponent underTest = new FileFeedCacheConnectionComponent("SomeEventStore", with(Optional.empty()));
        assertThat(underTest.getReport().getStatus(), CoreMatchers.equalTo(Status.CRITICAL));
        assertThat((String) underTest.getReport().getValue(),
                containsString("Successfully connected to File Feed EventStore Archive, but no EventStore Archive with ID='SomeEventStore' exists"));
    }

    @Test public void reports_critical_when_it_cannoot_connect_to_retrieve_max_postion_for_an_archive() {
        FileFeedCacheConnectionComponent underTest = new FileFeedCacheConnectionComponent("SomeEventStore", with(new RuntimeException("You cannot connect")));
        assertThat(underTest.getReport().getStatus(), CoreMatchers.equalTo(Status.CRITICAL));
        assertThat((String) underTest.getReport().getValue(),
                containsString("Unable to connect to File Feed EventStore Archive to retrieve max position"));
    }

    private MaxPositionFetcher with(Optional<BasicMysqlEventStorePosition> position) {
        return new MaxPositionFetcher() {
            @Override public Optional<BasicMysqlEventStorePosition> maxPosition() { return position; }
            @Override public Optional<BasicMysqlEventStorePosition> maxPositionBefore(Instant time) { return Optional.empty(); }
        };
    }

    private MaxPositionFetcher with(RuntimeException e) {
        return new MaxPositionFetcher() {
            @Override public Optional<BasicMysqlEventStorePosition> maxPosition() { throw e; }
            @Override public Optional<BasicMysqlEventStorePosition> maxPositionBefore(Instant time) { return Optional.empty(); }
        };
    }

}