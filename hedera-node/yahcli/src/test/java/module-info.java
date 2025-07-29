// SPDX-License-Identifier: Apache-2.0
module com.hedera.node.yahcli.test {
    requires com.hedera.node.yahcli;
    requires org.assertj.core;
    requires org.junit.jupiter.params;

    // For test visibility
    opens com.hedera.services.yahcli.test to
            org.junit.platform.commons,
            org.junit.jupiter.api,
            org.assertj.core;
    opens com.hedera.services.yahcli.test.config to
            org.junit.platform.commons;
}
