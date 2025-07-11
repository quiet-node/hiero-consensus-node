// SPDX-License-Identifier: Apache-2.0
module org.hiero.otter.fixtures {
    requires transitive com.hedera.node.hapi;
    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.common;
    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.logging;
    requires transitive com.swirlds.platform.core;
    requires transitive com.swirlds.state.api;
    requires transitive com.swirlds.state.impl;
    requires transitive org.hiero.base.utility;
    requires transitive org.hiero.consensus.model;
    requires transitive com.google.common;
    requires transitive com.google.protobuf;
    requires transitive io.grpc.stub;
    requires transitive io.grpc;
    requires transitive org.apache.logging.log4j.core;
    requires transitive org.apache.logging.log4j;
    requires transitive org.assertj.core;
    requires transitive org.junit.jupiter.api;
    requires transitive org.testcontainers;
    requires com.hedera.node.app.hapi.utils;
    requires com.hedera.node.config;
    requires com.swirlds.base.test.fixtures;
    requires com.swirlds.base;
    requires com.swirlds.common.test.fixtures;
    requires com.swirlds.component.framework;
    requires com.swirlds.config.extensions;
    requires com.swirlds.merkledb;
    requires com.swirlds.metrics.api;
    requires com.swirlds.platform.core.test.fixtures;
    requires org.hiero.base.crypto;
    requires org.hiero.consensus.utility;
    requires awaitility;
    requires io.grpc.protobuf;
    requires java.net.http;
    requires org.junit.jupiter.params;
    requires org.junit.platform.commons;
    requires static com.github.spotbugs.annotations;

    exports org.hiero.otter.fixtures;
    exports org.hiero.otter.fixtures.assertions;
    exports org.hiero.otter.fixtures.junit;
    exports org.hiero.otter.fixtures.logging;
    exports org.hiero.otter.fixtures.result;
    exports org.hiero.otter.fixtures.container.proto;
    exports org.hiero.otter.fixtures.app;
    exports org.hiero.otter.fixtures.logging.internal to
            org.hiero.consensus.otter.docker.app;
}
