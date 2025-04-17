// SPDX-License-Identifier: Apache-2.0
module org.hiero.consensus.utility {
    exports org.hiero.consensus.config;
    exports org.hiero.consensus.roster;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.consensus.model;
    requires static transitive com.github.spotbugs.annotations;
}
