import com.timgroup.sbtjavaversion.SbtJavaVersionKeys._

version in ThisBuild := "0.0." + sys.env.getOrElse("BUILD_NUMBER", "0-SNAPSHOT")

organization in ThisBuild := "com.timgroup"

scalaVersion in ThisBuild := "2.11.8"

crossScalaVersions := Seq("2.11.8", "2.12.1")

javaVersion in ThisBuild := "1.8"

javacOptions += "-g"
javacOptions += "-parameters"

publishTo in ThisBuild := Some("publish-repo" at "http://repo.youdevise.com:8081/nexus/content/repositories/yd-release-candidates")

credentials in ThisBuild += Credentials(new File("/etc/sbt/credentials"))

resolvers in ThisBuild += "TIM Group Repo" at "http://repo.youdevise.com/nexus/content/groups/public"

val joda = Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.3.1")

val compatibleScalaTestDependency = libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

val JUnit = Seq(
  "junit" % "junit" % "4.12" % "test",
  "com.timgroup" % "clocks-testing" % autobump % "test",
  "com.novocode" % "junit-interface" % "0.10-M1" % "test",
  "org.hamcrest" % "hamcrest-core" % "1.3" % "test",
  "org.hamcrest" % "hamcrest-library" % "1.3" % "test"
)

val eventstore_api = Project(id = "eventstore-api", base = file("api"))
  .settings(libraryDependencies += "com.timgroup" % "Tucker" % autobump)
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
    libraryDependencies += "com.typesafe" % "config" % "1.2.1",
    libraryDependencies += "c3p0" % "c3p0" % "0.9.1.2",
    libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test",
    libraryDependencies += "com.timgroup" %% "tim-slogger" % autobump
  )
  .settings(CreateDatabase.settings :_*)
  .settings(overridePublishSettings)

val eventstore_stitching = Project(id = "eventstore-stitching", base = file("stitching"))
  .dependsOn(eventstore_api % "compile->compile; test->test", eventstore_memory % "test")
  .settings(libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2")
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)
  .settings(
    libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "19.0"
))

val eventstore_ges_http = Project(id = "eventstore-ges-http", base = file("ges-http"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .settings(libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2")
  .settings(libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2")
  .settings(libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.2")
  .settings(publishArtifact in (Compile, packageDoc) := false)
  .settings(overridePublishSettings)

val eventstore_subscription = Project(id = "eventstore-subscription", base = file("subscription"))
  .dependsOn(eventstore_api, eventstore_memory % "compile->test")
  .settings(
    publishArtifact in (Compile, packageDoc) := false,
    libraryDependencies ++= Seq(
      "com.lmax" % "disruptor" % "3.3.2",
      "com.timgroup" % "tim-structured-events" % autobump,
      "com.timgroup" % "tim-structured-events-testing" % autobump % "test",
      "org.mockito" % "mockito-core" % "1.9.5" % "test",
      "com.youdevise" % "Matchers" % autobump
))
  .settings(overridePublishSettings)
