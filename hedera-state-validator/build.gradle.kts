// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.application")
}

dependencies{
    implementation("com.google.code.gson:gson:2.10")
}

mainModuleInfo {
    annotationProcessor("com.swirlds.config.processor")
    annotationProcessor("com.google.auto.service.processor")
    annotationProcessor("org.projectlombok:lombok:1.18.30")

    runtimeOnly("org.hyperledger.besu:besu-datatypes:24.3.3")
    runtimeOnly("org.bouncycastle:bcprov-jdk18on:1.77")
    runtimeOnly("org.bouncycastle:bcpkix-jdk18on:1.77")
    runtimeOnly("com.swirlds.config.impl")
    /*
     Traditional libraries that provide no module information at allfor example commons-cli:commons-cli:1.4.
     Gradle puts such libraries on the classpath instead of the module path.
     The classpath is then treated as one module (the so called unnamed module) by Java.
    */
    // https://mvnrepository.com/artifact/org.javassist/javassist

    requires("info.picocli:picocli:4.7.0")
    requires("com.goterl:lazysodium-java:5.1.1")
    requires("org.eclipse.collections:eclipse-collections:10.4.0")
    requires("io.github.classgraph:classgraph:4.8.65")
    requires("org.apache.logging.log4j:log4j-api:2.17.2")
    requires("org.apache.logging.log4j:log4j-core:2.17.2")
    requires("io.tmio:tuweni-bytes:2.4.2")
    requires("com.hedera.pbj:pbj-runtime:0.9.20")
    requires("com.google.protobuf:protobuf-java:3.17.3")
    requires("javax.inject:javax.inject:1")
    requires("com.google.dagger:dagger:2.51")
    requires("org.apache.commons:commons-lang3:3.14.0")
    requires("org.mockito:mockito-core:5.8.0")
    requires("com.lmax:disruptor:3.4.4")
    requires("io.github.artsok:rerunner-jupiter:2.1.6")
    requires("com.hedera.cryptography:hedera-cryptography-pairings-api:0.2.0-SNAPSHOT")
    requires("com.hedera.cryptography:hedera-cryptography-tss:0.2.0-SNAPSHOT")
    requires("io.github.java-diff-utils:java-diff-utils:4.12")
    requires ("com.hedera.node.app.hapi.fees")
    requires ("com.hedera.node.app.hapi.utils")
    requires ("com.hedera.node.app.service.addressbook.impl")
    requires ("com.hedera.node.app.service.addressbook")
    requires ("com.hedera.node.app.service.contract.impl")
    requires ("com.hedera.node.app.service.schedule.impl")
    requires ("com.hedera.node.app.service.schedule")
    requires ("com.hedera.node.app.service.token.impl")
    requires ("com.hedera.node.app.service.token")
    requires ("com.hedera.node.app.spi")
    requires ("com.hedera.node.app.test.fixtures")
    requires ("com.hedera.node.app")
    requires ("com.hedera.node.config")
    requires ("com.hedera.node.hapi")
    requires ("com.swirlds.base.test.fixtures")
    requires ("com.swirlds.base")
    requires ("com.swirlds.common")
    requires ("com.swirlds.config.api")
    requires ("com.swirlds.merkledb")
    requires ("com.swirlds.metrics.api")
    requires ("com.swirlds.platform.core.test.fixtures")
    requires ("com.swirlds.platform.core")
    requires ("com.swirlds.state.api")
    requires ("com.swirlds.virtualmap")
    requires ("org.hiero.base.concurrent")
    requires ("org.hiero.base.crypto")
    requires ("org.hiero.base.utility")
    requires ("org.hiero.consensus.model")
    requires ("org.hiero.consensus.utility")
    requires ("com.hedera.pbj.runtime")
    // https://mvnrepository.com/artifact/io.prometheus/simpleclient_httpserver
    requires("io.prometheus:simpleclient_httpserver:0.16.0")
    // https://mvnrepository.com/artifact/io.prometheus/simpleclient
    requires("io.prometheus:simpleclient:0.16.0")
    requires("io.github.java-diff-utils:java-diff-utils:4.12")


    // Define the individual libraries
    // JUnit Bundle
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.mockito.junit.jupiter")
    requires("org.junit.platform.launcher")
    // Mocking Bundle
    requires( "org.mockito:mockito-core:4.7.0")
    requires( "org.mockito:mockito-junit-jupiter:3.23.1")

    // Utils Bundle
    requires( "org.assertj:assertj-core:3.23.1")

    // lombok
    requires("org.projectlombok:lombok:1.18.30")
    requires("com.swirlds.virtualmap")
    requires("info.picocli")

}

application.mainClass = "com.hedera.statevalidation.validators.Validator"
