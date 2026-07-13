package com.timgroup.gradle

import org.gradle.api.artifacts.ComponentMetadataContext
import org.gradle.api.artifacts.ComponentMetadataRule

// see https://docs.gradle.org/current/userguide/component_capabilities.html#sec:declaring-capabilities-external-modules
class LoggingCapability implements ComponentMetadataRule {
    void execute(ComponentMetadataContext context) {
        context.details.with {
            if (id.group == "org.slf4j") {
                if (id.name == "jcl-over-slf4j") {
                    // Declare overlapping capability with clogger
                    allVariants {
                        it.withCapabilities {
                            it.addCapability("commons-logging", "commons-logging", "${id.version}@slf4j")
                        }
                    }
                }
                if (id.name == "log4j-over-slf4j") {
                    // Declare overlapping capability with log4j
                    allVariants {
                        it.withCapabilities {
                            it.addCapability("log4j", "log4j", "${id.version}@slf4j")
                        }
                    }
                }
            }
        }
    }
}
