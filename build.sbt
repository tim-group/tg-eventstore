import scala.util.matching.Regex

name := "mysql-eventstore"

version := "0.0." + sys.env.getOrElse("BUILD_NUMBER", "0-SNAPSHOT")

scalaVersion := "2.11.5"

crossScalaVersions := Seq("2.9.1", "2.10.3", "2.11.5")

libraryDependencies += "joda-time" % "joda-time" % "2.3"

libraryDependencies += "org.joda" % "joda-convert" % "1.3.1"

libraryDependencies <<= (libraryDependencies, scalaVersion) { (ld, sv) =>
  (sv match {
    case "2.11.5" => Seq(
      "org.scalatest" %% "scalatest" % "2.1.3" % "test",
      "org.scala-lang.modules" %% "scala-xml" % "1.0.2" % "test")
    case "2.10.3" => Seq(
      "org.scalatest" %% "scalatest" % "2.1.3" % "test"
    )
    case "2.9.1" => Seq(
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "test"
    )
    case _ => Seq()
  }) ++ ld
}

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test"

CreateDatabase.settings

net.virtualvoid.sbt.graph.Plugin.graphSettings
