// SPDX-License-Identifier: Apache-2.0
module com.hedera.node.test.clients.yahcli.test {
//    requires com.hedera.node.test.clients;
	requires com.hedera.services.yahcli;
    requires info.picocli;
    requires org.apache.logging.log4j;
    requires org.junit.jupiter.api;
	requires org.junit.jupiter.params;
	requires com.hedera.node.app;
	requires com.hedera.node.hapi;
	requires org.hiero.base.utility;
	requires com.github.spotbugs.annotations;
	requires com.google.common;
	requires com.google.protobuf;
	requires net.i2p.crypto.eddsa;
	requires org.yaml.snakeyaml;

	// For test visibility
	opens com.hedera.services.yahcli.test to
			org.junit.platform.commons, org.junit.jupiter.api;

}
