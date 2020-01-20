package com.timgroup.eventstore.archiver;

import org.junit.Test;

import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class S3ArchiveKeyFormatTest {

    @Test public void
    can_encode_position_into_key_name() {
        S3ArchiveKeyFormat format = new S3ArchiveKeyFormat("MyShinyEventStore");

        String key = format.objectKeyFor(42L, ".txt");
        assertThat(format.positionValueFrom(key), equalTo(42L));
    }

    @Test public void
    keys_sort_by_position_that_is_encoded_into_them() {
        S3ArchiveKeyFormat format = new S3ArchiveKeyFormat("MyShinyEventStore");

        Stream<String> sortedByKey = Stream.of(
                1_000L,
                1L,
                20L,
                3L,
                9_999L)
                .map(position -> format.objectKeyFor(position, ".txt"))
                .sorted();

        assertThat(sortedByKey.map(format::positionValueFrom).collect(toList()),
                equalTo(asList(1L, 3L, 20L, 1_000L, 9_999L)));
    }

    @Test public void
    key_prefix_terminates_event_store_id_to_prevent_ambiguity() {
        S3ArchiveKeyFormat format = new S3ArchiveKeyFormat("MyShinyEventStore");
        assertThat(format.eventStorePrefix(), equalTo("MyShinyEventStore/"));
    }

}