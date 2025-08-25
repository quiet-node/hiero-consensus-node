// SPDX-License-Identifier: Apache-2.0
module org.hiero.telemetryconverter {
    requires transitive com.hedera.node.hapi;
    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.base;
    requires org.eclipse.collections.api;
    requires org.eclipse.collections.impl;
    requires jdk.jfr;
    requires io.grpc.protobuf;
    requires com.hedera.pbj.grpc.client.helidon;
    requires io.helidon.common.tls;
    requires io.helidon.webclient.api;
}
