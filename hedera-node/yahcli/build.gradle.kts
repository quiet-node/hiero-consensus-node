// SPDX-License-Identifier: Apache-2.0
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("org.hiero.gradle.module.application")
    id("org.hiero.gradle.feature.shadow")
}

description = "Hedera Services Test Clients for YahCli"

mainModuleInfo {
    runtimeOnly("org.junit.jupiter.engine")
    runtimeOnly("org.junit.platform.launcher")
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-exports")
}

tasks.compileJava {
    dependsOn(":test-clients:assemble")
}

tasks.test {
    useJUnitPlatform {}

    // Tell our launcher to target an embedded network whose mode is set per-class
    systemProperty("hapi.spec.embedded.mode", "per-class")

    // Limit heap and number of processors
    maxHeapSize = "8g"
    jvmArgs("-XX:ActiveProcessorCount=6")
}

tasks.register<ShadowJar>("yahCliJar") {
    exclude(listOf("META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.SF", "META-INF/INDEX.LIST"))
    archiveClassifier.set("yahcli-shadow")

    manifest { attributes("Main-Class" to "com.hedera.services.yahcli.Yahcli") }
    from(sourceSets["main"].output)
    from(project(":test-clients").tasks.named("jar").get().outputs.files)

    // allow shadow Jar files to have more than 64k entries
    isZip64 = true
}

val cleanYahCli =
    tasks.register<Delete>("cleanYahCli") {
        group = "copy"
        delete(File(project.file("yahcli"), "yahcli.jar"))
    }

tasks.clean {
    dependsOn(cleanYahCli)
}
