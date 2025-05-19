// SPDX-License-Identifier: Apache-2.0
import com.hedera.pbj.compiler.DefaultPbjSourceDirectorySet
import com.hedera.pbj.compiler.PbjCompilerTask
import com.hedera.pbj.compiler.PbjSourceDirectorySet

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
    id("com.hedera.pbj.pbj-compiler") version "0.11.0"
}

description = "Consensus Otter Test Framework"

testModuleInfo {
    requires("com.swirlds.logging")
    requires("org.apache.logging.log4j")
    requires("org.hiero.consensus.utility")
    requires("org.hiero.otter.fixtures")
    requires("org.assertj.core")
}

// Setup for test fixtures: code copied from PBJ plugin until
// https://github.com/hashgraph/pbj/issues/493 is resolved.
sourceSets.testFixtures {
    val outputDirectory = "generated/source/pbj-proto/"
    val pbjSourceDirectorySet =
        objects.newInstance(
            DefaultPbjSourceDirectorySet::class,
            objects.sourceDirectorySet("testFixtures pbj", "testFixtures pbj"),
        )
    pbjSourceDirectorySet.srcDir("src/${name}/proto")
    allSource.source(pbjSourceDirectorySet)
    extensions.add(PbjSourceDirectorySet::class, PbjSourceDirectorySet.NAME, pbjSourceDirectorySet)

    val pbjCompiler =
        tasks.register<PbjCompilerTask>(getTaskName("generate", "PbjSource")) {
            description = "Processes the testFixtures Pbj grammars."
            source = pbjSourceDirectorySet
            javaMainOutputDirectory =
                layout.buildDirectory.dir(outputDirectory + "testFixtures/java")
            javaPackageSuffix = project.pbj.javaPackageSuffix
            // TODO with https://github.com/hiero-ledger/hiero-consensus-node/pull/19026 merged:
            // generateTestClasses = false

            // note: the following is unused, but the plugin currently expects it
            javaTestOutputDirectory =
                layout.buildDirectory.dir(outputDirectory + "testFixturesTest/java")

            // Workaround: https://github.com/hashgraph/pbj/issues/494
            // We include the files we depend on from the 'hedera-protobuf-java-api' project. This
            // is the only way to make them available to PBJ at the moment. PBJ will then also
            // generate code for these files which we delete in a doLast {} action.
            source(File(rootDir, "hapi/hedera-protobuf-java-api/src/main/proto"))
            include("*.proto") // files in this project
            include("platform/event/state_signature_transaction.proto") // dependency
            include("services/timestamp.proto") // dependency
            doLast {
                javaMainOutputDirectory.get().dir("com/hedera/hapi").asFile.deleteRecursively()
            }
        }
    java.srcDir(pbjCompiler.flatMap(PbjCompilerTask::getJavaMainOutputDirectory))
}
