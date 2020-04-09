package com.timgroup.eventstore.mysql;

import java.time.Instant;
import java.util.Optional;

public interface MaxPositionFetcher {

    Optional<BasicMysqlEventStorePosition> maxPosition();

    Optional<BasicMysqlEventStorePosition> maxPositionBefore(Instant cutOff);

}
