pluginManagement {
    repositories {
        gradlePluginPortal()
        val repoUrl: String by settings
        maven(url = "$repoUrl/groups/public")
    }
}

rootProject.name = "tg-eventstore"

include("api")
include("api:testlib")
include("api-legacy")
include("api-legacy:testlib")
include("diffing")
include("memory")
include("memory-legacy")
include("mysql")
include("mysql-legacy")
include("filesystem")
include("filesystem:archivetool")
include("filesystem:file-feed-cache")
include("stitching")
include("stitching-legacy")
include("ges-http")
include("subscription")
include("subscription-legacy")
include("archiver")
