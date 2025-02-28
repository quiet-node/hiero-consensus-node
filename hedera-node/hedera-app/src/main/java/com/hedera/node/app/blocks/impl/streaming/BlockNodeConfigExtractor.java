// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import com.hedera.node.internal.network.BlockNodeConfig;
import com.hedera.node.internal.network.BlockNodeConnectionInfo;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Extracts block node configuration from a JSON configuration file.
 */
public class BlockNodeConfigExtractor {
    private static final Logger logger = LogManager.getLogger(BlockNodeConfigExtractor.class);
    private final Map<Integer, List<BlockNodeConfig>> nodesByPriority = Map.of();
    private final int maxSimultaneousConnections = 1;
    private final List<BlockNodeConfig> allNodes;
    private final int blockItemBatchSize;

    /**
     * @param blockNodeConfigPath the path to the block node configuration file
     */
    public BlockNodeConfigExtractor(String blockNodeConfigPath) {
        final var configPath = Paths.get(blockNodeConfigPath, "block-nodes.json");

        try {
            byte[] jsonConfig = Files.readAllBytes(configPath);
            BlockNodeConnectionInfo protoConfig = BlockNodeConnectionInfo.JSON.parse(Bytes.wrap(jsonConfig));

            // Convert proto config to internal config objects
            this.allNodes = protoConfig.nodes().stream()
                    .map(node -> new BlockNodeConfig(node.address(), node.port()))
                    .collect(Collectors.toList());

            logger.info("Loaded block node configuration from {}", configPath);
            logger.info("Block node configuration: {}", allNodes);

            this.blockItemBatchSize = protoConfig.blockItemBatchSize();

        } catch (IOException | ParseException e) {
            logger.error("Failed to read block node configuration from {}", configPath, e);
            throw new RuntimeException("Failed to read block node configuration from " + configPath, e);
        }
    }

    public List<BlockNodeConfig> getBestAvailableNodes() {
        List<Integer> sortedPriorities = new ArrayList<>(nodesByPriority.keySet());
        Collections.sort(sortedPriorities); // Lower priority numbers first

        ExecutorService executor = Executors.newFixedThreadPool(maxSimultaneousConnections);

        try {
            for (Integer priority : sortedPriorities) {
                List<BlockNodeConfig> group = new ArrayList<>(nodesByPriority.get(priority));
                Collections.shuffle(group); // Randomize order in the group

                List<Future<BlockNodeConfig>> futures = new ArrayList<>();

                for (BlockNodeConfig node : group) {
                    if (futures.size() < maxSimultaneousConnections) {
                        futures.add(executor.submit(() -> canConnect(node) ? node : null));
                    }
                }

                List<BlockNodeConfig> successfulConnections = new ArrayList<>();
                for (Future<BlockNodeConfig> future : futures) {
                    try {
                        BlockNodeConfig connectedNode = future.get();
                        if (connectedNode != null) {
                            successfulConnections.add(connectedNode);
                        }
                    } catch (Exception e) {
                        // Handle connection failure
                    }
                }

                if (!successfulConnections.isEmpty()) {
                    executor.shutdown();
                    return successfulConnections;
                }
            }
        } finally {
            executor.shutdown();
        }

        throw new RuntimeException("No available nodes to connect to!");
    }

    /**
     * @param node the block node to connect to
     * @return true if the connection was successful, false otherwise
     */
    private boolean canConnect(BlockNodeConfig node) {
        // Connect to a randomly selected node in the highest priority group available

        // TODO Should not be here in the final implementation
        return ThreadLocalRandom.current().nextBoolean();
    }

    /**
     * @return the list of all block node configurations
     */
    public List<BlockNodeConfig> getAllNodes() {
        return allNodes;
    }

    /**
     * @return the block items batch size to send to the block nodes
     */
    public int getBlockItemBatchSize() {
        return blockItemBatchSize;
    }
}
