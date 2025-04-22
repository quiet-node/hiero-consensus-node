// SPDX-License-Identifier: Apache-2.0
open module org.hiero.consensus.utility.test.fixtures {
    exports org.hiero.consensus.utility.test.fixtures.event;

    requires transitive com.hedera.node.hapi;
    requires transitive com.hedera.pbj.runtime;
    requires transitive org.hiero.consensus.model;
    requires org.hiero.base.crypto.test.fixtures;
    requires org.hiero.base.crypto;
    requires org.hiero.base.utility.test.fixtures;
    requires org.hiero.consensus.utility;
    requires com.github.spotbugs.annotations;
    requires java.desktop;
}
