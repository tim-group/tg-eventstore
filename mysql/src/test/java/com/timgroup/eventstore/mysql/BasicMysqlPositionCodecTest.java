package com.timgroup.eventstore.mysql;

import java.util.Arrays;
import java.util.List;

import com.timgroup.eventstore.api.Position;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BasicMysqlPositionCodecTest {
    @Test
    public void serializes_position_as_string() throws Exception {
        assertThat(BasicMysqlEventStorePosition.CODEC.serializePosition(new BasicMysqlEventStorePosition(12345L)), equalTo("12345"));
    }

    @Test
    public void deserializes_position_from_string() throws Exception {
        assertThat(BasicMysqlEventStorePosition.CODEC.deserializePosition("12345"), equalTo(new BasicMysqlEventStorePosition(12345L)));
    }

    @Test
    public void orders_positions_numerically() throws Exception {
        List<Position> positions = Arrays.asList(
                new BasicMysqlEventStorePosition(10L),
                new BasicMysqlEventStorePosition(2L),
                new BasicMysqlEventStorePosition(1L),
                new BasicMysqlEventStorePosition(100L)
        );
        assertThat(positions.stream().sorted(BasicMysqlEventStorePosition.CODEC::comparePositions).collect(toList()), equalTo(Arrays.asList(
                new BasicMysqlEventStorePosition(1L),
                new BasicMysqlEventStorePosition(2L),
                new BasicMysqlEventStorePosition(10L),
                new BasicMysqlEventStorePosition(100L)
        )));
    }
}
