package com.timgroup.eventstore.mysql

import java.sql.Connection

import org.slf4j.LoggerFactory

/**
  * @deprecated uaw LegacyMysqlEventSource instead
  */
@Deprecated
private[mysql] object ResourceManagement {
  private val logger = LoggerFactory.getLogger(getClass)

  def transactionallyUsing[T](connectionProvider: ConnectionProvider)(code: Connection => T): T = {
    withResource(connectionProvider.getConnection()) { connection =>
      try {
        connection.setAutoCommit(false)
        val result = code(connection)
        connection.commit()
        result
      } catch {
        case e: Exception =>
          connection.rollback()
          throw e
      }
    }
  }

  def withResource[A <: AutoCloseable, B](open: => A)(usage: A => B): B = {
    val resource = open
    var throwing = false
    try {
      usage(resource)
    } catch {
      case ex: Throwable =>
        throwing = true
        try {
          resource.close()
        } catch {
          case rex: Throwable =>
            logger.info(s"Failure closing $resource", rex)
            ex.addSuppressed(rex)
        }
        throw ex
    } finally {
      if (!throwing) {
        try {
          resource.close()
        } catch {
          case rex: Throwable =>
            logger.info(s"Failure closing $resource (IGNORED)", rex)
        }
      }
    }
  }
}
