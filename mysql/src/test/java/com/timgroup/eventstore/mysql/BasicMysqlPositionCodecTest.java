package com.timgroup.eventstore.mysql;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BasicMysqlPositionCodecTest {
    @Test
    public void serializes_position_as_string() throws Exception {
        assertThat(new BasicMysqlPositionCodec().serializePosition(new BasicMysqlEventStorePosition(12345L)), equalTo("12345"));
    }

    @Test
    public void deserializes_position_from_string() throws Exception {
        assertThat(new BasicMysqlPositionCodec().deserializePosition("12345"), equalTo(new BasicMysqlEventStorePosition(12345L)));
    }
}
