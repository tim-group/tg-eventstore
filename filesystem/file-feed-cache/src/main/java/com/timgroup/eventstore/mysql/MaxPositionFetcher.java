package com.timgroup.eventstore.mysql;

import java.util.Optional;

public interface MaxPositionFetcher {

    Optional<BasicMysqlEventStorePosition> maxPosition();

}
