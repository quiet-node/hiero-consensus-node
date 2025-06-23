// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.test;

import static org.assertj.core.api.Assertions.fail;
import static org.hiero.otter.fixtures.OtterAssertions.assertThat;

import com.swirlds.base.time.Time;
import com.swirlds.common.io.utility.FileUtils;
import com.swirlds.platform.event.preconsensus.PcesEventFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import jdk.jfr.Description;
import org.apache.logging.log4j.Level;
import org.hiero.otter.fixtures.Network;
import org.hiero.otter.fixtures.Node;
import org.hiero.otter.fixtures.OtterTest;
import org.hiero.otter.fixtures.TestEnvironment;
import org.hiero.otter.fixtures.TimeManager;
import org.junit.jupiter.api.BeforeAll;

/**
 * Test class for verifying the behavior of
 */
public class PcesMigrationNonFreezeTest {

    private static final Duration DURATION = Duration.ofMinutes(10);

    static Path stateSnapshotTmpDir;

    @BeforeAll
    static void setup() throws IOException {
        stateSnapshotTmpDir = Files.createTempDirectory("state");
    }
    /**
     * This is not a test, this a prerequisite for next tests:
     * <pre>
     * 1. Run a network
     * 2. Generate some transactions and wait for enough time to pass so that snapshot is generated
     * 3. Shoot down the network without the freeze
     * 4. Save the latest snapshot to a particular directory
     * </pre>
     *
     * Enabling deterministic key generation allows for restarted nodes to use the same keys.
     *
     * @param env the test environment for this test
     * @throws InterruptedException if an operation times out
     */
    @OtterTest
    @Description("First step of the testing process, will generate a state to reload")
    void testA(final TestEnvironment env) throws InterruptedException {

        final Network network = env.network();
        network.withDeterministicKeyGeneration(true);
        final TimeManager timeManager = env.timeManager();
        // Setup simulation
        network.addNodes(4);
        for (final Node node : network.getNodes()) {
            node.getConfiguration().set("virtualMap.copyFlushThreshold", "100");
        }
        network.start();
        env.transactionGenerator().start();

        timeManager.waitFor(DURATION);

        network.shutdown();

        network.copyStateSnapshotTo(stateSnapshotTmpDir);
    }

    /**
     * Tests steps
     * <pre>
     * 1. Given a snapshot previously generated that is not a freeze state, use the {@link PcesEventFilter} to filter Pces Events
     * 2. Run the network using a snapshot previously generated with higher number of nodes than the original network that generated the state
     * 3. Validate that the nodes are not branching
     * </pre>
     *
     * @param env the test environment for this test
     * @throws InterruptedException if an operation times out
     */
    @OtterTest
    @Description(
            "Test that reloading a non freeze existing state into a new network with more nodes does not fail after filtering the events in pces")
    void testC(final TestEnvironment env) throws InterruptedException {
        final Network network = env.network();
        network.withDeterministicKeyGeneration(true);
        doFilterPcesFileEvents(env.timeManager().time());

        // Setup simulation
        network.addNodes(6);
        network.useInitialSnapshot(stateSnapshotTmpDir);
        network.start();
        env.timeManager().waitFor(DURATION);

        // todo: we should find another way of performing this assert
        assertThat(network.getLogResults()).hasNoLogThatMatchesLevelAndMessage(Level.ERROR, ".*Node \\d+ is branching");
        // Initiate the migration
        network.shutdown();
    }

    @OtterTest
    @Description("Test that reloading a non freeze existing state into a new network with more nodes does fail")
    void testB(final TestEnvironment env) throws InterruptedException {
        final Network network = env.network();
        network.withDeterministicKeyGeneration(true);
        // Setup simulation
        network.addNodes(6);
        network.useInitialSnapshot(stateSnapshotTmpDir);
        try {
            network.start();
        } catch (AssertionError e) {
            // todo: we should find another way of performing this assert
            assertThat(network.getLogResults())
                    .hasLogThatMatchesLevelAndMessage(Level.ERROR, ".*Node \\d+ is branching");
        }

        // Initiate the migration
        network.shutdown();
    }

    private static void doFilterPcesFileEvents(final Time time) {
        try {
            final Path stateDir =
                    FileUtils.listFilesAndDirs(stateSnapshotTmpDir).getLast();
            final Path pcesDir = stateDir.resolve("preconsensus-events");
            final Path pcesOutput = Files.createTempDirectory("pcesOutput");
            final Path output = pcesOutput.resolve("filtered");
            PcesEventFilter.create(stateDir).filter(pcesDir, output);
            Files.move(pcesDir, pcesOutput.resolve("original"));
            Files.move(output, pcesDir, StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            fail("Was unable to prepare state for filtering pces");
        }
    }
}
