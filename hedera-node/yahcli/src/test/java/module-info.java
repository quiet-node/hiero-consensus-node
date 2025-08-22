import com.hedera.services.yahcli.test.YahcliLauncherSessionListener;
import org.junit.platform.launcher.LauncherSessionListener;

module hiero.consensus.node.yahcli.test {
    requires com.github.spotbugs.annotations;
    requires com.google.protobuf;
    requires com.hedera.node.hapi;
    requires com.hedera.node.test.clients;
    requires com.hedera.node.yahcli;
    requires info.picocli;
    requires org.apache.logging.log4j;
    requires org.assertj.core;
    requires org.junit.jupiter.api;
    requires org.junit.jupiter.params;
    requires org.junit.platform.launcher;
    requires com.swirlds.config.api;
    requires org.eclipse.collections.api;
    requires io.github.classgraph;
    requires simpleclient;
    requires com.hedera.common.nativesupport;
    requires org.jspecify;
    requires javax.inject;
    requires jakarta.inject;
    requires io.helidon.common.resumable;
    requires io.helidon.common.buffers;
    requires io.helidon.common;

    provides LauncherSessionListener with
            YahcliLauncherSessionListener;

    opens com.hedera.services.yahcli.test
            to org.junit.platform.commons;
    opens com.hedera.services.yahcli.test.scenarios
            to org.junit.platform.commons;
}