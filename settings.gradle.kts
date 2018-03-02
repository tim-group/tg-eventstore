pluginManagement {
    repositories {
        gradlePluginPortal()
        maven(url = "http://repo.net.local/nexus/content/groups/public")
    }
}

rootProject.name = "tg-eventstore"

include("api")
include("api:testlib")
include("api-legacy")
include("api-legacy:testlib")
include("memory")
include("memory-legacy")
include("mysql")
include("mysql-legacy")
include("filesystem")
include("stitching")
include("stitching-legacy")
include("ges-http")
include("subscription")
include("subscription-legacy")
