// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.application")
    id("org.hiero.gradle.feature.shadow")
}

dependencies{
    runtimeOnly("org.hyperledger.besu:besu-datatypes:24.3.3")
    runtimeOnly("org.bouncycastle:bcprov-jdk18on:1.77")
    runtimeOnly("org.bouncycastle:bcpkix-jdk18on:1.77")
    implementation("com.google.code.gson:gson:2.10")
    implementation("info.picocli:picocli:4.7.0")
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

    // https://mvnrepository.com/artifact/io.prometheus/simpleclient_httpserver
    implementation("io.prometheus:simpleclient_httpserver:0.16.0")
    // https://mvnrepository.com/artifact/io.prometheus/simpleclient
    implementation("io.prometheus:simpleclient:0.16.0")

    // Mocking Bundle
    implementation( "org.mockito:mockito-core:4.7.0")
    implementation( "org.mockito:mockito-junit-jupiter:3.23.1")
    implementation("com.google.code.gson:gson:2.10")
}

mainModuleInfo {
    annotationProcessor("com.swirlds.config.processor")
    annotationProcessor("com.google.auto.service.processor")


    runtimeOnly("com.swirlds.config.impl")
    /*
     Traditional libraries that provide no module information at allfor example commons-cli:commons-cli:1.4.
     Gradle puts such libraries on the classpath instead of the module path.
     The classpath is then treated as one module (the so called unnamed module) by Java.
    */
    // https://mvnrepository.com/artifact/org.javassist/javassist


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
    requires("io.github.javadiffutils")



    // Define the individual libraries
    // JUnit Bundle
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.junit.jupiter.engine")
    requires("org.mockito.junit.jupiter")
    requires("org.junit.platform.launcher")

    requires("com.swirlds.virtualmap")
    requires("info.picocli")
    //requires("com.lmax.disruptor")
    requires("artsok.rerunner.jupiter")

}

application.mainClass = "com.hedera.statevalidation.validators.Validator"
