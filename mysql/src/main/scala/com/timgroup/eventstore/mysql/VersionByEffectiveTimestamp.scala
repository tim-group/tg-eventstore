package com.timgroup.eventstore.mysql

import java.sql.{Timestamp, ResultSet, PreparedStatement}

import org.joda.time.DateTime
import scala.util.control.Exception.allCatch

class VersionByEffectiveTimestamp(connectionProvider: ConnectionProvider, tableName: String = "Event") {
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
      allCatch opt { statement.close() }
      allCatch opt { connection.close() }
    }
  }
}
