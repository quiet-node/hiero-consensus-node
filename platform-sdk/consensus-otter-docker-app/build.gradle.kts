// SPDX-License-Identifier: Apache-2.0
plugins {
    id("java-library")
    id("jacoco")
    id("org.hiero.gradle.base.jpms-modules")
    id("org.hiero.gradle.base.lifecycle")
    id("org.hiero.gradle.base.version")
    id("org.hiero.gradle.check.dependencies")
    id("org.hiero.gradle.check.javac-lint")
    id("org.hiero.gradle.check.spotless")
    id("org.hiero.gradle.check.spotless-java")
    id("org.hiero.gradle.check.spotless-kotlin")
    id("org.hiero.gradle.feature.git-properties-file")
    id("org.hiero.gradle.feature.java-compile")
    id("org.hiero.gradle.feature.java-execute")
    id("org.hiero.gradle.feature.test")
    id("org.hiero.gradle.report.test-logger")
    id("org.hiero.gradle.feature.test-fixtures")
    id("org.hiero.gradle.feature.protobuf")
}

description = "Otter Docker App"

testFixturesModuleInfo {
    runtimeOnly("io.netty.transport.epoll.linux.x86_64")
    runtimeOnly("io.netty.transport.epoll.linux.aarch_64")
    runtimeOnly("io.helidon.grpc.core")
    runtimeOnly("io.helidon.webclient")
    runtimeOnly("io.helidon.webclient.grpc")
}

val testFixturesLibsForManifest =
    configurations.detachedConfiguration().apply {
        extendsFrom(configurations.testFixturesRuntimeClasspath.get())
        isTransitive = true
        attributes { attribute(Attribute.of("javaModule", Boolean::class.javaObjectType), false) }
    }

val testFixturesClassPath: String by lazy {
    testFixturesLibsForManifest
        .resolve()
        .filter { it.name.endsWith(".jar") }
        .sortedBy { it.name }
        .joinToString(" ") { "../lib/${it.name}" }
}

tasks.named<Jar>("testFixturesJar") {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes(
            "Main-Class" to "org.hiero.consensus.otter.docker.app.DockerInit",
            "Class-Path" to testFixturesClassPath,
        )
    }
}

val copyTestFixturesLib =
    tasks.register<Sync>("copyTestFixturesLib") {
        dependsOn(tasks.named("jar"))

        val artifactView =
            testFixturesLibsForManifest.incoming.artifactView {
                attributes {
                    attribute(Attribute.of("javaModule", Boolean::class.javaObjectType), false)
                }
            }

        artifactView.artifacts.forEach { artifact: ResolvedArtifactResult ->
            val id = artifact.id.componentIdentifier
            if (id is ProjectComponentIdentifier) {
                dependsOn("${id.projectPath}:jar")
            }
        }

        from(artifactView.files)
        into(layout.buildDirectory.dir("data/lib"))
    }

val copyTestFixturesApp =
    tasks.register<Sync>("copyTestFixturesApp") {
        from(tasks.named("testFixturesJar"))
        into(layout.buildDirectory.dir("data/apps"))
        rename { "DockerApp.jar" }
        shouldRunAfter(copyTestFixturesLib)
    }

tasks.assemble {
    dependsOn(copyTestFixturesLib)
    dependsOn(copyTestFixturesApp)
}
