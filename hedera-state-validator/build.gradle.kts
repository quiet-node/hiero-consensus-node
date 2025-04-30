/*
 * Copyright (C) 2023-2024 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.file.Paths

/*
 * Copyright (C) 2022 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    id("com.hedera.state.validator.conventions")
    id("com.hedera.state.validator.application")
    id("com.github.johnrengelman.shadow").version("7.1.2")
}
var hederaServicesDir = System.getProperty("hedera.services.dir", "./hedera-services")
var validatorVersion = project.findProperty("tool-version") as String? ?: "no-version"
version = validatorVersion

dependencies {
    implementation(project(":common"))
    implementation(testLibs.bundles.junit)
    /*
     Traditional libraries that provide no module information at allfor example commons-cli:commons-cli:1.4.
     Gradle puts such libraries on the classpath instead of the module path.
     The classpath is then treated as one module (the so called unnamed module) by Java.
    */
    // https://mvnrepository.com/artifact/org.javassist/javassist

    implementation("info.picocli:picocli:4.7.0")
    runtimeOnly("org.hyperledger.besu:besu-datatypes:24.3.3")

    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-base/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/hapi/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-common/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-config-api/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-config-impl/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-config-extensions/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-fchashmap/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-fcqueue/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-jasperdb/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-logging/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-merkle/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-merkledb/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-platform-core/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/event-creator-impl/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-state-api/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-state-impl/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-unit-tests/common/swirlds-test-framework/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-virtualmap/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-metrics-api/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )
    implementation(
        fileTree(
            mapOf(
                "dir" to Paths.get(hederaServicesDir, "/platform-sdk/swirlds-state-api/build/libs").toString(),
                "exclude" to listOf("*-sources.jar")
            )
        )
    )

    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hapi/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hapi-fees/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hapi-utils/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-file-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-addressbook-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-addressbook-service-impl/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-app-spi/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-app/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-config/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-consensus-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-consensus-service-impl/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-evm/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-file-service-impl/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-network-admin-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-network-admin-service-impl/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-schedule-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-schedule-service-impl/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-smart-contract-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-smart-contract-service-impl/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-token-service-impl/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-token-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-util-service/build/libs")))
    implementation(fileTree(Paths.get(hederaServicesDir, "/hedera-node/hedera-util-service-impl/build/libs")))

    runtimeOnly("org.bouncycastle:bcprov-jdk18on:1.77")
    runtimeOnly("org.bouncycastle:bcpkix-jdk18on:1.77")
    implementation("com.goterl:lazysodium-java:5.1.1")
    implementation("org.eclipse.collections:eclipse-collections:10.4.0")
    implementation("io.github.classgraph:classgraph:4.8.65")
    implementation("org.apache.logging.log4j:log4j-api:2.17.2")
    implementation("org.apache.logging.log4j:log4j-core:2.17.2")
    implementation("io.tmio:tuweni-bytes:2.4.2")
    implementation("com.hedera.pbj:pbj-runtime:0.9.20")
    implementation("com.google.protobuf:protobuf-java:3.17.3")
    implementation("javax.inject:javax.inject:1")
    implementation("com.google.dagger:dagger:2.51")
    implementation("org.apache.commons:commons-lang3:3.14.0")
    implementation("org.mockito:mockito-core:5.8.0")
    implementation("com.lmax:disruptor:3.4.4")
    implementation("io.github.artsok:rerunner-jupiter:2.1.6")
    implementation("com.hedera.cryptography:hedera-cryptography-pairings-api:0.2.0-SNAPSHOT")
    implementation("com.hedera.cryptography:hedera-cryptography-tss:0.2.0-SNAPSHOT")

    // https://mvnrepository.com/artifact/io.prometheus/simpleclient_httpserver
    implementation("io.prometheus:simpleclient_httpserver:0.16.0")
    // https://mvnrepository.com/artifact/io.prometheus/simpleclient
    implementation("io.prometheus:simpleclient:0.16.0")
    implementation("io.github.java-diff-utils:java-diff-utils:4.12")

    // lombok
    compileOnly("org.projectlombok:lombok:1.18.30")
    annotationProcessor("org.projectlombok:lombok:1.18.30")
}

sourceSets {
    main {
        resources {
            srcDir("src/main/resources")
            // Include the external file as part of resources
            srcDir(hederaServicesDir)
            include("version.txt")
            include("log4j2.xml")
        }
    }
}

tasks.named<Copy>("processResources") {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE // Exclude duplicate files
}

tasks.named<CreateStartScripts>("startScripts") {
    dependsOn(":validator:copyApp")
}

tasks.shadowJar {
    dependsOn(":validator:copyApp")
    archiveBaseName.set("validator")
    archiveVersion.set(validatorVersion)
    archiveClassifier.set("")
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://repo.maven.apache.org/maven2/")
    }
    maven {
        url = uri("https://oss.sonatype.org/content/repositories/snapshots")
    }
    maven {
        url = uri("https://us-maven.pkg.dev/swirlds-registry/maven-prerelease-channel")
    }
    maven {
        url = uri("https://us-maven.pkg.dev/swirlds-registry/maven-develop-commits")
    }
    maven {
        url = uri("https://us-maven.pkg.dev/swirlds-registry/maven-adhoc-commits")
    }
    maven {
        url = uri("https://us-maven.pkg.dev/swirlds-registry/maven-develop-daily-snapshots")
    }
    maven {
        url = uri("https://us-maven.pkg.dev/swirlds-registry/maven-develop-snapshots")
    }
    maven {
        url = uri("https://hyperledger.jfrog.io/artifactory/besu-maven")
        content { includeGroupByRegex("org\\.hyperledger\\..*") }
    }
    maven {
        url = uri("https://artifacts.consensys.net/public/maven/maven/")
        content { includeGroupByRegex("tech\\.pegasys(\\..*)?") }
    }
    maven {
        url = uri("https://oss.sonatype.org/content/repositories/comhederahashgraph-1502")
    }
    maven {
        url = uri("https://oss.sonatype.org/content/repositories/comhederahashgraph-1531")
    }
}
