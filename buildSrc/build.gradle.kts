plugins {
    `kotlin-dsl`
}

gradlePlugin {
    plugins {
        register("scala-properties-plugin") {
            id = "scala-properties"
            implementationClass = "ScalaPropertiesPlugin"
        }
    }
}

repositories {
    jcenter()
}
