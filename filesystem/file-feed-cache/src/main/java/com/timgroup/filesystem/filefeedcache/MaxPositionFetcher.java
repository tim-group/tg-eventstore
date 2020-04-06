package com.timgroup.filesystem.filefeedcache;

import java.util.Optional;

public interface MaxPositionFetcher {

    Optional<Long> maxPosition();

}
