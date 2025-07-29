// SPDX-License-Identifier: Apache-2.0
module com.hedera.node.yahcli.test {
    requires com.hedera.node.app;
    requires com.hedera.node.hapi;
    requires com.hedera.node.test.clients;
    requires com.hedera.node.yahcli;
    requires org.hiero.base.utility;
    requires com.github.spotbugs.annotations;
    requires com.google.common;
    requires com.google.protobuf;
    requires info.picocli;
    requires net.i2p.crypto.eddsa;
    requires org.apache.logging.log4j;
    requires org.assertj.core;
    requires org.junit.jupiter.api;
    requires org.junit.jupiter.params;
    requires org.yaml.snakeyaml;

    // For test visibility
    opens com.hedera.services.yahcli.test to
            org.junit.platform.commons,
            org.junit.jupiter.api,
            org.assertj.core;
    opens com.hedera.services.yahcli.test.config to
            org.junit.platform.commons;
}
