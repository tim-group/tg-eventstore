import sbt._
import Keys._

object CreateDatabase {
  val createDb = TaskKey[Unit]("create-db")

  val settings = Seq(TaskKey[Unit]("create-db") := {
    println("Re-creating DB")
    Process("mysql", Seq("-edrop database if exists sql_eventstore")).!
    Process("mysql", Seq("-ecreate database sql_eventstore")).!
    Process("mysql", Seq("sql_eventstore", "-eCREATE TABLE Event(eventType VARCHAR(255), body BLOB, version INT AUTO_INCREMENT PRIMARY KEY, effective_timestamp datetime)")).!
  },
    (test in Test) <<= (test in Test) dependsOn createDb
  )
}