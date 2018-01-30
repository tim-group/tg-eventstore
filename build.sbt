version in ThisBuild := "0.0." + sys.env.getOrElse("BUILD_NUMBER", "0-SNAPSHOT")

organization in ThisBuild := "com.timgroup"

scalaVersion in ThisBuild := "2.11.8"

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
  "com.timgroup" % "clocks-testing" % "1.0.1080" % "test", // autobump
  "com.novocode" % "junit-interface" % "0.10-M1" % "test",
  "org.hamcrest" % "hamcrest-core" % "1.3" % "test",
  "org.hamcrest" % "hamcrest-library" % "1.3" % "test"
)

val createDb = TaskKey[Unit]("create-db")

val publishJavaOnly = Seq(crossPaths := false,
                          autoScalaLibrary := false)

val publishScala = Seq(
  crossScalaVersions := Seq("2.11.8", "2.12.1")
)

val eventstore_api = Project(id = "eventstore-api", base = file("api"))
  .settings(
    libraryDependencies += "com.timgroup" % "Tucker" % "1.0.1494", // autobump
    libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "4.0.2")
  .settings(libraryDependencies ++= joda)
  .settings(libraryDependencies ++= JUnit)
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)
  .settings(publishJavaOnly)

val eventstore_api_legacy = Project(id = "eventstore-api-legacy", base = file("api-legacy"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)
  .settings(publishScala)

val eventstore_memory = Project(id = "eventstore-memory", base = file("memory"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .settings(overridePublishSettings)
  .settings(publishJavaOnly)

val eventstore_memory_legacy = Project(id = "eventstore-memory-legacy", base = file("memory-legacy"))
  .dependsOn(eventstore_api_legacy % "compile->compile; test->test", eventstore_memory)
  .settings(overridePublishSettings)
  .settings(publishScala)

val eventstore_mysql = Project(id = "eventstore-mysql", base = file("mysql"))
  .dependsOn(eventstore_api % "compile->compile; test->test", eventstore_memory % "test")
  .settings(
    parallelExecution in Test := false,
    compatibleScalaTestDependency,
    libraryDependencies += "com.typesafe" % "config" % "1.2.1",
    libraryDependencies += "c3p0" % "c3p0" % "0.9.1.2",
    libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test",
    libraryDependencies += "com.timgroup" % "tim-logger" % "1.5.1084" // autobump
  )
  .settings(libraryDependencies ++= JUnit)
  .settings(
    createDb := {
      println("Re-creating DB")
      Process("mysql", Seq("-edrop database if exists sql_eventstore")).!
      Process("mysql", Seq("-ecreate database sql_eventstore")).!
      Process("mysql", Seq("sql_eventstore", "-eCREATE TABLE Event(eventType VARCHAR(255), body BLOB, version INT PRIMARY KEY, effective_timestamp datetime)")).!
    }
  )
  .settings((test in Test) <<= (test in Test) dependsOn createDb)
  .settings(overridePublishSettings)
  .settings(publishJavaOnly)

val eventstore_mysql_legacy = Project(id = "eventstore-mysql-legacy", base = file("mysql-legacy"))
  .dependsOn(eventstore_api_legacy % "compile->compile; test->test", eventstore_mysql, eventstore_memory_legacy % "test")
  .settings(
    parallelExecution in Test := false,
    compatibleScalaTestDependency,
    libraryDependencies += "com.typesafe" % "config" % "1.2.1",
    libraryDependencies += "c3p0" % "c3p0" % "0.9.1.2",
    libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.20" % "test",
    libraryDependencies += "com.timgroup" % "tim-logger" % "1.5.1084" // autobump
  )
  .settings((test in Test) <<= (test in Test) dependsOn (createDb in eventstore_mysql))
  .settings(overridePublishSettings)
  .settings(publishScala)

val eventstore_stitching = Project(id = "eventstore-stitching", base = file("stitching"))
  .dependsOn(eventstore_api % "compile->compile; test->test", eventstore_memory % "test")
  .settings(libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2")
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)
  .settings(
    libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "19.0"
))
  .settings(publishJavaOnly)

val eventstore_stitching_legacy = Project(id = "eventstore-stitching-legacy", base = file("stitching-legacy"))
  .dependsOn(eventstore_api_legacy % "compile->compile; test->test", eventstore_stitching, eventstore_memory_legacy % "test")
  .settings(libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2")
  .settings(compatibleScalaTestDependency)
  .settings(overridePublishSettings)
  .settings(publishScala)
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
  .settings(publishJavaOnly)

val eventstore_filesystem = Project(id = "eventstore-filesystem", base = file("filesystem"))
  .dependsOn(eventstore_api % "compile->compile; test->test")
  .settings(publishArtifact in (Compile, packageDoc) := false)
  .settings(overridePublishSettings)
  .settings(publishJavaOnly)

val eventstore_subscription = Project(id = "eventstore-subscription", base = file("subscription"))
  .dependsOn(eventstore_api % "compile->compile; test->test", eventstore_memory % "test")
  .settings(
    publishArtifact in (Compile, packageDoc) := false,
    libraryDependencies ++= Seq(
      "com.lmax" % "disruptor" % "3.3.2",
      "com.timgroup" % "tim-structured-events" % "0.4.1235", // autobump
      "com.timgroup" % "tim-structured-events-testing" % "0.4.1235" % "test", // autobump
      "org.mockito" % "mockito-core" % "1.9.5" % "test",
      "com.youdevise" % "Matchers" % "0.0.1263" % "test" // autobump
))
  .settings(overridePublishSettings)
  .settings(publishJavaOnly)

val eventstore_subscription_legacy = Project(id = "eventstore-subscription-legacy", base = file("subscription-legacy"))
  .dependsOn(eventstore_api_legacy % "compile->compile; test->test", eventstore_subscription, eventstore_memory % "test")
  .settings(
    publishArtifact in (Compile, packageDoc) := false,
    libraryDependencies ++= Seq(
      "com.lmax" % "disruptor" % "3.3.2",
      "com.timgroup" % "tim-structured-events" % "0.4.1235", // autobump
      "com.timgroup" % "tim-structured-events-testing" % "0.4.1235" % "test", // autobump
      "org.mockito" % "mockito-core" % "1.9.5" % "test",
      "com.youdevise" % "Matchers" % "0.0.1263" % "test" // autobump
))
  .settings(overridePublishSettings)
  .settings(publishScala)
