import sbt._
import sbt.Keys._
import sbt.Package.ManifestAttributes

object EventStoreBuild extends Build {
  val defaults = Seq(
    version := "0.0." + sys.env.getOrElse("BUILD_NUMBER", "0-SNAPSHOT"),
    organization := "com.timgroup",
    scalaVersion := "2.11.5",
    crossScalaVersions := Seq("2.9.1", "2.10.3", "2.11.5"),
    parallelExecution in Global := false,
    publishTo := Some("publish-repo" at "http://repo.youdevise.com:8081/nexus/content/repositories/yd-release-candidates"),
    credentials += Credentials(new File("/etc/sbt/credentials")),
    packageOptions <<= (version, scalaVersion) map { (v, sv) => Seq(ManifestAttributes(("Implementation-Version", v + "_" + sv))) }
  )

  val joda = Seq(
    "joda-time" % "joda-time" % "2.3",
    "org.joda" % "joda-convert" % "1.3.1")

  val compatibleScalaTestDependency = libraryDependencies <<= (libraryDependencies, scalaVersion) { (ld, sv) =>
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

  val eventstore_api = Project(id = "eventstore-api", base = file("api"))
    .settings(defaults : _*)
    .settings(libraryDependencies ++= joda)
    .settings(compatibleScalaTestDependency)

  val eventstore_mysql = Project(id = "eventstore-mysql", base = file("mysql"))
    .dependsOn(eventstore_api % "compile->compile; test->test")
    .settings(defaults : _*)
    .settings(
      compatibleScalaTestDependency,
      libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test"
    )
    .settings(CreateDatabase.settings :_*)

  val eventstore_memory = Project(id = "eventstore-memory", base = file("memory"))
    .dependsOn(eventstore_api % "compile->compile; test->test")
    .settings(defaults : _*)
}