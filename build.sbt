import com.timgroup.sbtjavaversion.SbtJavaVersionKeys._

version in ThisBuild := "0.0." + sys.env.getOrElse("BUILD_NUMBER", "0-SNAPSHOT")

organization in ThisBuild := "com.timgroup"

scalaVersion in ThisBuild := "2.11.8"

javaVersion in ThisBuild := "1.8"

crossScalaVersions in ThisBuild := Seq("2.10.4", "2.11.8")

publishTo in ThisBuild := Some("publish-repo" at "http://repo.youdevise.com:8081/nexus/content/repositories/yd-release-candidates")

credentials in ThisBuild += Credentials(new File("/etc/sbt/credentials"))


val joda = Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.3.1")

val compatibleScalaTestDependency = libraryDependencies <<= (libraryDependencies, scalaVersion) { (ld, sv) =>
  (sv match {
    case "2.11.8" => Seq(
      "org.scalatest" %% "scalatest" % "2.1.3" % "test",
      "org.scala-lang.modules" %% "scala-xml" % "1.0.2" % "test")
    case "2.10.4" => Seq(
      "org.scalatest" %% "scalatest" % "2.1.3" % "test"
    )
    case "2.9.1" => Seq(
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "test"
    )
    case version => throw new RuntimeException("I don't know what scalatest to use for " + version)
  }) ++ ld
}

val eventstore_api = Project(id = "eventstore-api", base = file("api"))
  .settings(libraryDependencies ++= joda)
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)

val eventstore_mysql = Project(id = "eventstore-mysql", base = file("mysql"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .settings(
    parallelExecution in Test := false,
    compatibleScalaTestDependency,
    libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test",
    libraryDependencies += "com.timgroup" %% "tim-slogger" % autobump
  )
  .settings(CreateDatabase.settings :_*)
  .settings(overridePublishSettings)

val eventstore_memory = Project(id = "eventstore-memory", base = file("memory"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .settings(overridePublishSettings)

val eventstore_stitching = Project(id = "eventstore-stitching", base = file("stitching"))
  .dependsOn(eventstore_api % "compile->compile; test->test", eventstore_memory % "test")
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)

val eventstore_subscription = Project(id = "eventstore-subscription", base = file("subscription"))
  .dependsOn(eventstore_api, eventstore_memory % "compile->test")
  .settings(libraryDependencies ++= Seq(
  "com.timgroup" % "Tucker" % "autobump",
  "com.lmax" % "disruptor" % "3.3.2",
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
))
  .settings(overridePublishSettings)
