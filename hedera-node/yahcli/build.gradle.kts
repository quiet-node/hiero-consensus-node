// SPDX-License-Identifier: Apache-2.0
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.kotlin.dsl.get

plugins {
    id("org.hiero.gradle.module.application")
    id("org.hiero.gradle.feature.shadow")
}

description = "Hedera Services YahCli Tool"

mainModuleInfo {
    runtimeOnly("org.junit.jupiter.engine")
    runtimeOnly("org.junit.platform.launcher")
}

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

tasks.compileJava { dependsOn(":test-clients:assemble") }

tasks.register<ShadowJar>("yahcliJar") {
    archiveClassifier.set("shadow")

    manifest { attributes("Main-Class" to "com.hedera.services.yahcli.Yahcli") }

    // Required test-clients files:
    from({ zipTree(project(":test-clients").tasks.named("jar").get().outputs.files.singleFile) }) {
        include("**/*.class", "**/log4j2-test.xml")
        includeEmptyDirs = false
    }
    // Required yahcli files:
    from(sourceSets["main"].output) {
        exclude("**/genesis.pem")
        includeEmptyDirs = false
    }

    // allow shadow Jar files to have more than 64k entries
    isZip64 = true

    dependsOn(tasks.named("compileJava"), tasks.named("classes"), tasks.named("processResources"))
}

tasks.assemble {
    dependsOn(tasks.named("yahcliJar"))
}

tasks.test {
    useJUnitPlatform {}

    // Tell our launcher to target an embedded network whose mode is set per-class
    systemProperty("hapi.spec.embedded.mode", "per-class")

    // Limit heap and number of processors
    maxHeapSize = "8g"
    jvmArgs("-XX:ActiveProcessorCount=6")
}

// Disable `shadowJar` so it doesn't conflict with `yahcliJar`
tasks.named("shadowJar") {
    enabled = false
}
// Disable unneeded tasks
tasks.matching { it.group == "distribution" }.configureEach {
    enabled = false
}
