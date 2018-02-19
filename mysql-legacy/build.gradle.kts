plugins {
    java
    scala
    id("com.timgroup.jarmangit")
    id("com.github.maiflai.scalatest")
    `maven-publish`
}

val repoUrl by project
val repoUsername by project
val repoPassword by project

val buildNumber: String? by rootProject.extra
val scalaVersion: String by rootProject.extra
val scalaApiVersion: String by rootProject.extra

group = "com.timgroup"
if (buildNumber != null) version = "0.0.$buildNumber"

repositories {
    maven("$repoUrl/groups/public")
}

val SourceSet.scala: SourceDirectorySet
    get() = withConvention(ScalaSourceSet::class) { scala }

// https://chrismarks.wordpress.com/2013/07/31/compiling-a-mixed-scala-project-with-gradle/
for (sourceSetName in listOf("main", "test")) {
    java.sourceSets[sourceSetName].apply {
        scala.srcDirs("src/$sourceSetName/java")
        java.setSrcDirs(emptySet<File>())
    }
}

dependencies {
    compile(project(":api-legacy"))
    compile(project(":mysql"))
    compile("com.fasterxml.jackson.core:jackson-databind:2.7.2")
    compileOnly("org.joda:joda-convert:1.3.1")

    testCompile(project(":api-legacy:testlib"))
    testCompile(project(":memory-legacy"))
    testCompile("junit:junit:4.12")
    testCompile("org.hamcrest:hamcrest-core:1.3")
    testCompile("org.hamcrest:hamcrest-library:1.3")
    testCompile("com.timgroup:clocks-testing:1.0.1080") // autobump
    testCompile("org.scalatest:scalatest_$scalaApiVersion:3.0.1")
    testCompileOnly("org.joda:joda-convert:1.3.1")
    testRuntime("org.pegdown:pegdown:1.4.2")
    testCompile("mysql:mysql-connector-java:5.1.20")
}

tasks["check"].dependsOn("scalatest")

tasks.withType<JavaCompile> {
    sourceCompatibility = "1.8"
    targetCompatibility = "1.8"
    options.encoding = "UTF-8"
    options.isIncremental = true
    options.isDeprecation = true
    options.compilerArgs.add("-parameters")
}

tasks.withType<Jar> {
    manifest {
        attributes(mapOf(
                "Implementation-Title" to project.name,
                "Implementation-Version" to project.version,
                "Implementation-Vendor" to "TIM Group Ltd"
        ))
    }
}

val javadoc by tasks.getting(Javadoc::class)

val sourcesJar by tasks.creating(Jar::class) {
    classifier = "sources"
    from(java.sourceSets["main"].allSource)
}

val javadocJar by tasks.creating(Jar::class) {
    classifier = "javadoc"
    from(javadoc)
}

tasks["assemble"].apply {
    dependsOn(sourcesJar)
    dependsOn(javadocJar)
}

val scalatest by tasks.getting(Test::class) {
    maxParallelForks = 1
    dependsOn(":createDb")
}

publishing {
    repositories {
        if (repoUrl != null) {
            maven("$repoUrl/repositories/yd-release-candidates") {
                credentials {
                    username = repoUsername.toString()
                    password = repoPassword.toString()
                }
            }
        }
    }
    (publications) {
        "mavenJava"(MavenPublication::class) {
            artifactId = "eventstore-mysql-legacy_$scalaApiVersion"
            from(components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)
        }
    }
}
