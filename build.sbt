name := "mysql-eventstore"

version := "0.0." + sys.env.getOrElse("BUILD_NUMBER", "0-SNAPSHOT")

scalaVersion := "2.11.0"

libraryDependencies += "joda-time" % "joda-time" % "2.3"

libraryDependencies += "org.joda" % "joda-convert" % "1.3.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.2" % "test"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test"

CreateDatabase.settings

net.virtualvoid.sbt.graph.Plugin.graphSettings
