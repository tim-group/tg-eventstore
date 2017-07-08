package com.timgroup.eventstore.mysql;

import com.typesafe.config.Config;
import org.junit.Test;

import static com.typesafe.config.ConfigFactory.parseString;
import static com.typesafe.config.ConfigParseOptions.defaults;
import static com.typesafe.config.ConfigSyntax.PROPERTIES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsSame.sameInstance;

public class StacksConfiguredDataSourceTest {

    @Test
    public void only_creates_one_datasource_per_unique_configuration() {

        Config config = parseString(
                "hostname=localhost\n" +
                        "port=3306\n" +
                        "database=sql_eventstore\n" +
                        "username=\n" +
                        "password=\n" +
                        "driver=com.mysql.jdbc.Driver", defaults().setSyntax(PROPERTIES));

        assertThat(StacksConfiguredDataSource.pooledMasterDb(config), sameInstance(StacksConfiguredDataSource.pooledMasterDb(config)));
    }

}