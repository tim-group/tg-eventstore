import com.timgroup.eventstore.gradle.*

plugins {
    id("com.timgroup.jarmangit") version "1.1.86" apply false
    id("com.github.maiflai.scalatest") version "0.19" apply false
}

val scalaTarget by extra(determineScalaTarget())

val buildNumber: String? by extra(System.getenv("BUILD_NUMBER") ?: System.getenv("TRAVIS_BUILD_NUMBER"))

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
