package com.timgroup.eventstore.mysql;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionProvider {
    @Nonnull
    Connection getConnection() throws SQLException;
}
