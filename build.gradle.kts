plugins {
    id("com.timgroup.jarmangit") version "1.1.86" apply false
    id("com.github.maiflai.scalatest") version "0.19" apply false
}

val scalaTarget by project

data class ScalaTarget(val major: Int, val minor: Int, val patch: Int) {
    val version: String
        get() = "$major.$minor.$patch"
    val apiVersion: String
        get() = "$major.$minor"
}

val scalaTargetVersion =
    if (scalaTarget != null) {
        val matchResult = Regex("""(\d+)\.(\d+)\.(\d+)""").matchEntire(scalaTarget.toString()) ?: throw IllegalArgumentException("Invalid Scala target: $scalaTarget")
        ScalaTarget(matchResult.groupValues[1].toInt(), matchResult.groupValues[2].toInt(), matchResult.groupValues[3].toInt())
    }
    else {
        ScalaTarget(2, 12, 4)
    }
val scalaVersion by extra(scalaTargetVersion.version)
val scalaApiVersion by extra(scalaTargetVersion.apiVersion)

val buildNumber: String by extra(System.getenv("BUILD_NUMBER") ?: System.getenv("TRAVIS_BUILD_NUMBER"))

task("createDb") {
    doLast {
        fun mysqlExec(cmd: String, db: String? = null) {
            project.exec {
                if (db != null)
                    commandLine("mysql", db, "-e$cmd")
                else
                    commandLine("mysql", "-e$cmd")
                environment("MYSQL_HOST", "127.0.0.1")
            }
        }
        mysqlExec("drop database if exists sql_eventstore")
        mysqlExec("create database sql_eventstore")
        mysqlExec("CREATE TABLE Event(eventType VARCHAR(255), body BLOB, version INT PRIMARY KEY, effective_timestamp datetime)", db = "sql_eventstore")
    }
}

task("publishScala") {
    group = "publishing"
    description = "Publish all Scala subprojects"
}

subprojects {
    afterEvaluate {
        if (plugins.hasPlugin("scala")) {
            rootProject.tasks["publishScala"].dependsOn(tasks["publish"])
        }
    }
}
