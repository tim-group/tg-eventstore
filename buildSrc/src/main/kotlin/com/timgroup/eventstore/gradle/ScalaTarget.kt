package com.timgroup.eventstore.gradle

import org.gradle.api.Project

data class ScalaTarget(val major: Int, val minor: Int, val patch: Int) {
    val version: String
        get() = "$major.$minor.$patch"
    val apiVersion: String
        get() = "$major.$minor"
}

fun ScalaTarget(config: String?): ScalaTarget {
    return if (config != null) {
        val matchResult = Regex("""(\d+)\.(\d+)\.(\d+)""").matchEntire(config) ?: throw IllegalArgumentException("Invalid Scala target: $config")
        ScalaTarget(matchResult.groupValues[1].toInt(), matchResult.groupValues[2].toInt(), matchResult.groupValues[3].toInt())
    } else {
        ScalaTarget(2, 12, 4)
    }
}

fun Project.determineScalaTarget() = ScalaTarget(findProperty("scalaTarget")?.toString())
