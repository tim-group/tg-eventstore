val (source, scalaTarget) = findScalaTarget()

val versionMatch = Regex("""(\d+\.\d+)\.(\d+)""").matchEntire(scalaTarget) ?: error("Invalid scalaTarget: $scalaTarget")
val (major, minor) = versionMatch.destructured
extra.set("scalaVersion", "$major.$minor")

if (major == "2.9") {
    extra.set("scalaApiVersion", "$major.$minor")
} else {
    extra.set("scalaApiVersion", major)
}

val scalaVersion: String by extra
val scalaApiVersion: String by extra
logger.info("Scala: using ($source) library $scalaVersion and API version $scalaApiVersion")

fun findScalaTarget(): Pair<String, String> {
    val scalaTarget = properties["scalaTarget"]?.toString()
    return if (scalaTarget != null) {
        "specified" to scalaTarget
    } else {
        "default" to file("scala_targets.txt").useLines { it.first() }
    }
}
