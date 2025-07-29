// SPDX-License-Identifier: Apache-2.0
module com.hedera.node.yahcli.test {
    requires com.hedera.node.yahcli;
    requires org.assertj.core;
    requires org.junit.jupiter.params;
	requires info.picocli;
	requires com.google.protobuf;
	requires com.hedera.node.test.clients;
	requires com.hedera.node.hapi;

    // For test visibility
    opens com.hedera.services.yahcli.test to
            org.junit.platform.commons,
            org.junit.jupiter.api,
            org.assertj.core;
    opens com.hedera.services.yahcli.test.config to
            org.junit.platform.commons;
}
