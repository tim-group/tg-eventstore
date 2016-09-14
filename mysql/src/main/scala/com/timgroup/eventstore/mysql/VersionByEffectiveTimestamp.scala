package com.timgroup.eventstore.mysql

import java.sql.Timestamp

import com.timgroup.eventstore.mysql.ResourceManagement.withResource
import org.joda.time.DateTime

class VersionByEffectiveTimestamp(connectionProvider: ConnectionProvider, tableName: String = "Event") {
  def versionFor(cutoff: DateTime): Long =
    withResource(connectionProvider.getConnection()) { connection =>
      withResource(connection.prepareStatement("select max(version) from " + tableName + " where effective_timestamp < ?")) { statement =>
        statement.setTimestamp(1, new Timestamp(cutoff.getMillis))
        withResource(statement.executeQuery()) { resultSet =>
          resultSet.next()
          resultSet.getLong(1)
        }
      }
    }
}
