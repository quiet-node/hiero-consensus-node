// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.suites.blocknode;

import static com.hedera.services.bdd.junit.TestTags.BLOCK_NODE_SIMULATOR;
import static com.hedera.services.bdd.junit.hedera.NodeSelector.byNodeId;
import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.cryptoCreate;
import static com.hedera.services.bdd.spec.utilops.BlockNodeSimulatorVerbs.blockNodeSimulator;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.assertHgcaaLogContains;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.sleepFor;
import static com.hedera.services.bdd.suites.HapiSuite.ONE_HUNDRED_HBARS;

import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
import com.hedera.services.bdd.junit.HapiTest;
import com.hedera.services.bdd.junit.hedera.BlockNodeMode;
import com.hedera.services.bdd.junit.hedera.WithBlockNodes;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
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
    final Stream<DynamicTest> node0BlockNodeInternalError() {
        AtomicLong lastVerifiedBlockNumber = new AtomicLong(0);

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
                blockNodeSimulator()
                        .sendEndOfStreamImmediately(0, PublishStreamResponseCode.STREAM_ITEMS_INTERNAL_ERROR)
                        .withBlockNumber(123456L)
                        .exposingLastVerifiedBlockNumber(lastVerifiedBlockNumber),
                // Verify the log message in node 0's log
                // TODO This is a temporary solution. Behaviors could be verified through log statements that occur
                // on the consensus node. In addition, the lastVerifiedBlockNumber could be used in the log verification
                // below.
                assertHgcaaLogContains(
                        byNodeId(0), "Error returned from block node at block number 123456", Duration.ofSeconds(5)));
    }

    @HapiTest
    final Stream<DynamicTest> node0BlockNodeShutsDownAndRestarts() {
        return hapiTest(
                // Shut down block node simulator 0
                blockNodeSimulator().shutDownImmediately(0),
                // Verify the log message in node 0's log
                assertHgcaaLogContains(byNodeId(0), "Error in block node stream", Duration.ofSeconds(5)),
                sleepFor(2000),
                // Restart node 0
                blockNodeSimulator().restartImmediately(0));
        // TODO Add more log assretions for reconnection
    }

    @HapiTest
    final Stream<DynamicTest> assertGenesisBlockReceivedBySimulator() {
        return hapiTest(blockNodeSimulator().assertBlockReceived(0, 0));
    }

    @HapiTest
    final Stream<DynamicTest> getLastVerifiedBlockFromSimulator() {
        AtomicLong lastVerifiedBlockNumber = new AtomicLong(0);

        return hapiTest(
                // Create a crypto account to generate some blocks
                cryptoCreate("account1")
                        .balance(ONE_HUNDRED_HBARS)
                        .declinedReward(true)
                        .stakedNodeId(0),
                // Wait a bit to ensure the block is processed
                sleepFor(2000),
                // Get the last verified block number using the fluent API
                blockNodeSimulator()
                        .getLastVerifiedBlock(0)
                        .exposingLastVerifiedBlockNumber(lastVerifiedBlockNumber)
                        .build(),
                // Assert that the block has been received by the simulator
                blockNodeSimulator().assertBlockReceived(0, lastVerifiedBlockNumber.get()));
    }
}
