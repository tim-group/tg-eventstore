plugins {
    `java-library`
    id("com.timgroup.jarmangit")
    `maven-publish`
}

val repoUrl by project
val repoUsername by project
val repoPassword by project

val buildNumber: String? by rootProject.extra

group = "com.timgroup"
if (buildNumber != null) version = "0.0.$buildNumber"

repositories {
    maven("$repoUrl/groups/public")
}

dependencies {
    api(project(":api"))
    api("com.typesafe:config:1.2.1")
    api("com.mchange:c3p0:0.9.5.2")
    compileOnly("com.google.code.findbugs:jsr305:1.3.9")
    implementation("com.timgroup:tim-logger:1.5.1086") // autobump
    implementation("io.dropwizard.metrics:metrics-core:3.2.3")

    testCompile(project(":api:testlib"))
    testCompile(project(":memory"))
    testCompile("junit:junit:4.12")
    testCompile("org.hamcrest:hamcrest-core:1.3")
    testCompile("org.hamcrest:hamcrest-library:1.3")
    testCompile("com.timgroup:clocks-testing:1.0.1080") // autobump
    testCompile("mysql:mysql-connector-java:5.1.20")
    testCompileOnly("com.google.code.findbugs:jsr305:1.3.9")
}

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

tasks["test"].dependsOn(":createDb")

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
            artifactId = "eventstore-mysql"
            from(components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)
        }
    }
}
