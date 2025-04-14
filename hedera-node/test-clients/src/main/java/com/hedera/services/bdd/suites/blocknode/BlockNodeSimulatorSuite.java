// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.suites.blocknode;

import static com.hedera.services.bdd.junit.TestTags.BLOCK_NODE_SIMULATOR;
import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.waitUntilNextBlock;

import com.hedera.services.bdd.HapiBlockNode;
import com.hedera.services.bdd.HapiBlockNode.BlockNodeConfig;
import com.hedera.services.bdd.HapiBlockNode.SubProcessNodeConfig;
import com.hedera.services.bdd.junit.HapiTest;
import com.hedera.services.bdd.junit.OrderedInIsolation;
import com.hedera.services.bdd.junit.hedera.BlockNodeMode;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * This suite is for testing with the block node simulator.
 *
 * <p>Note: This suite requires the block node simulator to be enabled.
 * Use the system property "hapi.spec.blocknode.mode=SIM" to enable it.
 * The testSubprocessWithBlockNodeSimulator task automatically sets this property.
 */
@Tag(BLOCK_NODE_SIMULATOR)
@OrderedInIsolation
public class BlockNodeSimulatorSuite {

    @HapiTest
    @HapiBlockNode(
            blockNodeConfigs = {@BlockNodeConfig(nodeId = 0, mode = BlockNodeMode.SIMULATOR)},
            subProcessNodeConfigs = {
                @SubProcessNodeConfig(
                        nodeId = 0,
                        blockNodeIds = {0},
                        simulatorPriorities = {0})
            })
    final Stream<DynamicTest> node0StreamingHappyPath() {
        return hapiTest(
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true));
    }

    @HapiTest
    @HapiBlockNode(
            blockNodeConfigs = {
                @BlockNodeConfig(nodeId = 0, mode = BlockNodeMode.SIMULATOR),
                @BlockNodeConfig(nodeId = 1, mode = BlockNodeMode.SIMULATOR),
                @BlockNodeConfig(nodeId = 2, mode = BlockNodeMode.SIMULATOR),
                @BlockNodeConfig(nodeId = 3, mode = BlockNodeMode.SIMULATOR),
            },
            subProcessNodeConfigs = {
                @SubProcessNodeConfig(
                        nodeId = 0,
                        blockNodeIds = {0},
                        simulatorPriorities = {0}),
                @SubProcessNodeConfig(
                        nodeId = 1,
                        blockNodeIds = {1},
                        simulatorPriorities = {0}),
                @SubProcessNodeConfig(
                        nodeId = 2,
                        blockNodeIds = {2},
                        simulatorPriorities = {0}),
                @SubProcessNodeConfig(
                        nodeId = 3,
                        blockNodeIds = {3},
                        simulatorPriorities = {0}),
            })
    final Stream<DynamicTest> allNodesStreamingHappyPath() {
        return hapiTest(
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true),
                waitUntilNextBlock().withBackgroundTraffic(true));
    }
}
