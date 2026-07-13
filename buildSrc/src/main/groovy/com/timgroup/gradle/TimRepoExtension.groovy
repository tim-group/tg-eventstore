package com.timgroup.gradle

import org.gradle.api.Project
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider
import org.gradle.api.provider.ProviderFactory

import javax.inject.Inject

class TimRepoExtension {
    final Provider<String> version
    final Provider<String> group
    final Provider<String> nexusRepoUrl
    final Provider<String> nexusRepoUsername
    final Provider<String> nexusRepoPassword
    final Provider<String> codeartifactUrl
    final Provider<String> codeartifactToken
    final Property<String> artifactId

    @Inject
    TimRepoExtension(ObjectFactory objectFactory, ProviderFactory providers, Project project) {
        def versionPrefix = providers.gradleProperty("versionPrefix")
        def buildNumber = providers.environmentVariable("ORIGINAL_BUILD_NUMBER")
                .orElse(providers.environmentVariable("BUILD_NUMBER"))
        version = versionPrefix.zip(buildNumber, (p, n) -> (String) "${p}.${n}")
        group = providers.gradleProperty("artifactGroup").orElse("com.timgroup")
        nexusRepoUrl = providers.gradleProperty("repoUrl")
        nexusRepoUsername = providers.gradleProperty("repoUsername")
        nexusRepoPassword = providers.gradleProperty("repoPassword")
        codeartifactUrl = providers.environmentVariable("CODEARTIFACT_URL")
                .orElse(providers.gradleProperty("codeartifact.url"))
                .orElse("https://timgroup-148217964156.d.codeartifact.eu-west-1.amazonaws.com/maven/jars/")
        codeartifactToken = providers.environmentVariable("CODEARTIFACT_TOKEN")
                .orElse(providers.gradleProperty("codeartifact.token"))
        artifactId = objectFactory.property(String).convention(providers.provider {
            def rootProject = project.rootProject
            if (project == rootProject)
                return project.name
            else
                return "${rootProject.name}-${project.name}"
        })
    }
}
