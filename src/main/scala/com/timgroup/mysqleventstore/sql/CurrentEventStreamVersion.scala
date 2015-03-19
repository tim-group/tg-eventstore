package com.timgroup.mysqleventstore.sql

import java.sql.Connection

object CurrentEventStreamVersion {
  def fetchCurrentVersion(connection: Connection, tableName: String) = {
    val statement = connection.prepareStatement("select max(version) from " + tableName)
    val results = statement.executeQuery()

    try {
      results.next()
      results.getLong(1)
    } finally {
      results.close()
      statement.close()
    }
  }
}
