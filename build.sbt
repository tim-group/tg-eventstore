import com.timgroup.sbtjavaversion.SbtJavaVersionKeys._

version in ThisBuild := "0.0." + sys.env.getOrElse("BUILD_NUMBER", "0-SNAPSHOT")

organization in ThisBuild := "com.timgroup"

scalaVersion in ThisBuild := "2.11.8"

javaVersion in ThisBuild := "1.8"

javacOptions += "-g"
javacOptions += "-parameters"

publishTo in ThisBuild := Some("publish-repo" at "http://repo.youdevise.com:8081/nexus/content/repositories/yd-release-candidates")

credentials in ThisBuild += Credentials(new File("/etc/sbt/credentials"))


val joda = Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.3.1")

val compatibleScalaTestDependency = libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.3" % "test",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.2" % "test"
)

val JUnit = Seq(
  "junit" % "junit" % "4.12" % "test",
  "com.timgroup" % "clocks-testing" % autobump % "test",
  "com.novocode" % "junit-interface" % "0.10-M1" % "test",
  "org.hamcrest" % "hamcrest-core" % "1.3" % "test",
  "org.hamcrest" % "hamcrest-library" % "1.3" % "test"
)

val eventstore_api = Project(id = "eventstore-api", base = file("api"))
  .settings(libraryDependencies ++= joda)
  .settings(libraryDependencies ++= JUnit)
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)

val eventstore_memory = Project(id = "eventstore-memory", base = file("memory"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .settings(overridePublishSettings)

val eventstore_mysql = Project(id = "eventstore-mysql", base = file("mysql"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .dependsOn(eventstore_api, eventstore_memory % "compile->test")
  .settings(
    parallelExecution in Test := false,
    compatibleScalaTestDependency,
    libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test",
    libraryDependencies += "com.timgroup" %% "tim-slogger" % autobump
  )
  .settings(CreateDatabase.settings :_*)
  .settings(overridePublishSettings)

val eventstore_stitching = Project(id = "eventstore-stitching", base = file("stitching"))
  .dependsOn(eventstore_api % "compile->compile; test->test", eventstore_memory % "test")
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)
  .settings(
    libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "19.0"
))

val eventstore_subscription = Project(id = "eventstore-subscription", base = file("subscription"))
  .dependsOn(eventstore_api, eventstore_memory % "compile->test")
  .settings(
    publishArtifact in (Compile, packageDoc) := false,
    libraryDependencies ++= Seq(
  "com.timgroup" % "Tucker" % autobump,
  "com.lmax" % "disruptor" % "3.3.2",
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
))
  .settings(overridePublishSettings)
