package com.swirlds.common.poc;

import static com.swirlds.common.poc.impl.EventGenerator.UNLIMITED;
import static com.swirlds.common.poc.impl.Validator.EventStreamConfig.ignoreNode;
import static com.swirlds.common.poc.impl.Validator.LogErrorConfig.ignoreMarkers;
import static com.swirlds.common.poc.impl.Validator.RatioConfig.within;
import static com.swirlds.logging.legacy.LogMarker.SOCKET_EXCEPTIONS;
import static com.swirlds.logging.legacy.LogMarker.TESTING_EXCEPTIONS_ACCEPTABLE_RECONNECT;

import com.swirlds.common.poc.impl.ConsensusTest;
import com.swirlds.common.poc.impl.EventGenerator.Distribution;
import com.swirlds.common.poc.impl.EventGenerator.Rate;
import com.swirlds.common.poc.impl.InstrumentedNode;
import com.swirlds.common.poc.impl.Network;
import com.swirlds.common.poc.impl.Node;
import com.swirlds.common.poc.impl.TestEnvironment;
import com.swirlds.common.poc.impl.TimeManager;
import com.swirlds.common.poc.impl.Validator.Profile;
import java.time.Duration;
import java.util.List;

public class SandboxTest {

    private static final Duration TEN_SECONDS = Duration.ofSeconds(10);
    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);
    private static final Duration TWO_MINUTES = Duration.ofMinutes(2);

    @ConsensusTest
    void testConsistencyNDReconnect(TestEnvironment env) {
        final Network network = env.network();
        final TimeManager timeManager = env.timeManager();

        // Setup simulation
        final List<Node> nodes = network.addNodes(4);
        network.start(ONE_MINUTE);
        env.generator().generateTransactions(UNLIMITED, Rate.regularRateWithTps(1000), Distribution.UNIFORM);

        // Wait for two minutes
        timeManager.waitFor(TWO_MINUTES);

        // Kill node
        final Node node = nodes.getFirst();
        node.kill(ONE_MINUTE);

        // Wait for two minutes
        timeManager.waitFor(TWO_MINUTES);

        // Revive node
        node.revive(ONE_MINUTE);

        // Wait for two minutes
        timeManager.waitFor(TWO_MINUTES);

        // Validations
        env.validator()
                .assertLogErrors(ignoreMarkers(SOCKET_EXCEPTIONS, TESTING_EXCEPTIONS_ACCEPTABLE_RECONNECT))
                .assertStdOut()
                .eventStream(ignoreNode(node))
                .reconnectEventStream(node)
                .validateRemaining(Profile.DEFAULT);
    }

    @ConsensusTest
    void testBranching(TestEnvironment env) {
        final Network network = env.network();
        final TimeManager timeManager = env.timeManager();

        // Setup simulation
        network.addNodes(3);
        final InstrumentedNode nodeX =
                network.addInstrumentedNode();
        network.start(ONE_MINUTE);
        env.generator().generateTransactions(
                UNLIMITED,
                Rate.regularRateWithTps(1000),
                Distribution.UNIFORM);

        // Wait for one minutes
        timeManager.waitFor(TEN_SECONDS);

        // Start branching
        nodeX.setBranchingProbability(0.5);

        // Wait for 10,000 events
        timeManager.waitForEvents(10_000);

        // Validations
        env.validator()
                .consensusRatio(within(0.8, 1.0))
                .staleRatio(within(0.0, 0.1))
                .validateRemaining(Profile.HASHGRAPH);
    }

}
