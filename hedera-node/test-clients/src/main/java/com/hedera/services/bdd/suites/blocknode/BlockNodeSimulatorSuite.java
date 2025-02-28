// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.suites.blocknode;

import static com.hedera.services.bdd.junit.TestTags.BLOCK_NODE_SIMULATOR;
import static com.hedera.services.bdd.junit.hedera.NodeSelector.byNodeId;
import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.cryptoCreate;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.assertHgcaaLogContains;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.withOpContext;
import static com.hedera.services.bdd.suites.HapiSuite.ONE_HUNDRED_HBARS;

import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
import com.hedera.services.bdd.junit.HapiTest;
import com.hedera.services.bdd.junit.hedera.BlockNodeMode;
import com.hedera.services.bdd.junit.hedera.WithBlockNodes;
import com.hedera.services.bdd.junit.hedera.simulator.BlockNodeSimulatorController;
import com.hedera.services.bdd.junit.hedera.subprocess.SubProcessNetwork;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * This suite is for testing with the block node simulator.
 */
@Tag(BLOCK_NODE_SIMULATOR)
@WithBlockNodes(BlockNodeMode.SIMULATOR)
public class BlockNodeSimulatorSuite {

    @HapiTest
    final Stream<DynamicTest> simulatorsAsExpected() {
        return hapiTest(cryptoCreate("simpleAccount")
                .balance(ONE_HUNDRED_HBARS)
                .declinedReward(true)
                .stakedNodeId(0));
    }

    @HapiTest
    final Stream<DynamicTest> node0BlockNodeInternalError() {
        return hapiTest(
                // Create a couple of crypto accounts
                cryptoCreate("account1")
                        .balance(ONE_HUNDRED_HBARS)
                        .declinedReward(true)
                        .stakedNodeId(0),
                cryptoCreate("account2")
                        .balance(ONE_HUNDRED_HBARS)
                        .declinedReward(true)
                        .stakedNodeId(0),
                // Use the simulator controller to make node 0 respond with internal error
                withOpContext((spec, log) -> {
                    SubProcessNetwork network = (SubProcessNetwork) spec.targetNetworkOrThrow();
                    BlockNodeSimulatorController controller = network.getBlockNodeSimulatorController();

                    // Send an immediate EndOfStream response with INTERNAL_ERROR
                    controller.sendEndOfStreamImmediately(
                            0, PublishStreamResponseCode.STREAM_ITEMS_INTERNAL_ERROR, 123456L);

                    log.info("Sent STREAM_ITEMS_INTERNAL_ERROR to block node simulator 0");
                    Thread.sleep(5000);
                }),
                // Verify the log message in node 0's log
                assertHgcaaLogContains(
                        byNodeId(0), "Error returned from block node at block number 123456", Duration.ofSeconds(5)));
    }
}
