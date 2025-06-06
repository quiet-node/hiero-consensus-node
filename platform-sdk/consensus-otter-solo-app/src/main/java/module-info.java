// SPDX-License-Identifier: Apache-2.0
module org.hiero.consensus.otter.solo {
    exports org.hiero.consensus.otter.solo;

    requires transitive com.hedera.node.hapi;
    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.common;
    requires transitive com.swirlds.platform.core;
    requires transitive org.hiero.consensus.model;
    requires com.swirlds.logging;
    requires org.apache.logging.log4j;
    requires static transitive com.github.spotbugs.annotations;
}
