package com.timgroup.eventstore.mysql

import java.sql.{Timestamp, ResultSet, PreparedStatement}

import org.joda.time.DateTime

class VersionByEffectiveTimestamp(connectionProvider: ConnectionProvider, tableName: String = "Event") extends CloseWithLogging {
  def versionFor(cuttoff: DateTime): Long = {
    val connection = connectionProvider.getConnection()
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null

    try {
      statement = connection.prepareStatement("select max(version) from " + tableName + " where effective_timestamp < ?")
      statement.setTimestamp(1, new Timestamp(cuttoff.getMillis))
      resultSet = statement.executeQuery()
      resultSet.next()

      resultSet.getLong(1)
    } finally {
      closeWithLogging(statement)
      closeWithLogging(connection)
    }
  }
}
