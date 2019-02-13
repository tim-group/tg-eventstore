import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.*

class ScalaPropertiesPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.run {
            val (source, scalaTarget) = findScalaTarget()
            val versionMatch = Regex("""(\d+\.\d+)\.(\d+)""").matchEntire(scalaTarget) ?: throw RuntimeException("Invalid scalaTarget: $scalaTarget")
            val (major, minor) = versionMatch.destructured
            extra.set("scalaVersion", "$major.$minor")
            if (major == "2.9") {
                extra.set("scalaApiVersion", "$major.$minor")
            }
            else {
                extra.set("scalaApiVersion", major)
            }

            val scalaVersion: String by extra
            val scalaApiVersion: String by extra
            logger.info("Scala: using ($source) library $scalaVersion and API version $scalaApiVersion")
        }
    }

    private fun Project.findScalaTarget(): Pair<String, String> {
        val scalaTarget: String? by project
        return scalaTarget?.let { "specified" to it } ?: "default" to project.file("scala_targets.txt").useLines { it.first() }
    }
}
