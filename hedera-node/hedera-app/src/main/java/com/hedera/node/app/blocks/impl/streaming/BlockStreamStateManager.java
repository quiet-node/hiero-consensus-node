// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.protoc.BlockItemSet;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.data.BlockStreamConfig;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages the state of blocks being streamed to block nodes.
 * This class is responsible for maintaining the block states and providing methods for adding items to blocks
 * and creating requests.
 */
public class BlockStreamStateManager {
    private static final Logger logger = LogManager.getLogger(BlockStreamStateManager.class);

    private final Map<Long, BlockState> blockStates = new ConcurrentHashMap<>();
    private final int blockItemBatchSize;

    // Reference to the connection manager for notifications
    private BlockNodeConnectionManager blockNodeConnectionManager;

    /**
     * Creates a new BlockStreamStateManager with the given configuration.
     *
     * @param configProvider the configuration provider
     */
    public BlockStreamStateManager(@NonNull final ConfigProvider configProvider) {
        requireNonNull(configProvider, "configProvider must not be null");
        final var blockStreamConfig = configProvider.getConfiguration().getConfigData(BlockStreamConfig.class);

        BlockNodeConfigExtractor blockNodeConfigurations =
                new BlockNodeConfigExtractor(blockStreamConfig.blockNodeConnectionFileDir());
        this.blockItemBatchSize = blockNodeConfigurations.getBlockItemBatchSize();
    }

    /**
     * Sets the block node connection manager for notifications.
     *
     * @param blockNodeConnectionManager the block node connection manager
     */
    public void setBlockNodeConnectionManager(@NonNull BlockNodeConnectionManager blockNodeConnectionManager) {
        requireNonNull(blockNodeConnectionManager, "blockNodeConnectionManager must not be null");
        this.blockNodeConnectionManager = blockNodeConnectionManager;
    }

    /**
     * Opens a new block with the given block number.
     *
     * @param blockNumber the block number
     * @throws IllegalArgumentException if the block number is negative
     */
    public void openBlock(long blockNumber) {
        if (blockNumber < 0) throw new IllegalArgumentException("Block number must be non-negative");
        // Create a new block state
        blockStates.put(blockNumber, BlockState.from(blockNumber));
        logger.info("Started new block in BlockStreamStateManager {}", blockNumber);
    }

    /**
     * Adds a new item to the current block.
     *
     * @param blockNumber the block number
     * @param bytes the item bytes to add
     * @throws IllegalStateException if no block is currently open
     */
    public void addItem(final long blockNumber, @NonNull Bytes bytes) {
        requireNonNull(bytes, "bytes must not be null");
        BlockState blockState = getBlockState(blockNumber);
        if (blockState == null) {
            throw new IllegalStateException("Block state not found for block " + blockNumber);
        }

        blockState.itemBytes().add(bytes);

        // If we have enough items, create a new request
        if (blockState.itemBytes().size() >= blockItemBatchSize) {
            createRequestFromCurrentItems(blockState);
        }
    }

    /**
     * Creates a new PublishStreamRequest from the current items in the block.
     */
    private void createRequestFromCurrentItems(@NonNull BlockState blockState) {
        List<com.hedera.hapi.block.stream.protoc.BlockItem> protocBlockItems = new ArrayList<>();
        for (Bytes batchItem : blockState.itemBytes()) {
            try {
                protocBlockItems.add(com.hedera.hapi.block.stream.protoc.BlockItem.parseFrom(batchItem.toByteArray()));
            } catch (IOException e) {
                logger.error("Failed to parse block item", e);
                throw new RuntimeException(e);
            }
        }

        // Create BlockItemSet by adding all items at once
        BlockItemSet itemSet =
                BlockItemSet.newBuilder().addAllBlockItems(protocBlockItems).build();

        // Create the request and add it to the list
        PublishStreamRequest request =
                PublishStreamRequest.newBuilder().setBlockItems(itemSet).build();

        blockState.requests().add(request);
        logger.info(
                "Added request to block {} - request count now: {}",
                blockState.blockNumber(),
                blockState.requests().size());

        // Clear the item bytes list
        blockState.itemBytes().clear();

        // Notify the connection manager
        blockNodeConnectionManager.notifyConnectionsOfNewRequest();
    }

    /**
     * Closes the current block and marks it as complete.
     * @param blockNumber the block number
     * @throws IllegalStateException if no block is currently open
     */
    public void closeBlock(final long blockNumber) {
        BlockState blockState = getBlockState(blockNumber);
        if (blockState == null) {
            throw new IllegalStateException("Block state not found for block " + blockNumber);
        }

        // Check if there are remaining items
        if (!blockState.itemBytes().isEmpty()) {
            logger.info(
                    "Creating request from remaining items in block {} size {}",
                    blockNumber,
                    blockState.itemBytes().size());
            createRequestFromCurrentItems(blockState);
        }

        // Mark the block as complete
        blockState.setComplete();
        logger.info(
                "Closed block in BlockStreamStateManager {} - request count: {}",
                blockNumber,
                blockState.requests().size());
    }

    /**
     * Gets the block state for the given block number.
     *
     * @param blockNumber the block number
     * @return the block state, or null if no block state exists for the given block number
     */
    public BlockState getBlockState(long blockNumber) {
        return blockStates.get(blockNumber);
    }

    /**
     * Creates a new request from the current items in the block prior to BlockProof if there are any.
     * @param blockNumber the block number
     */
    public void performPreBlockProofActions(long blockNumber) {
        BlockState blockState = getBlockState(blockNumber);
        if (blockState == null) {
            throw new IllegalStateException("Block state not found for block " + blockNumber);
        }

        // If there are remaining items we will create a request from them while the BlockProof is pending
        if (!blockState.itemBytes().isEmpty()) {
            logger.info(
                    "Prior to BlockProof, creating request from items in block {} size {}",
                    blockNumber,
                    blockState.itemBytes().size());
            createRequestFromCurrentItems(blockState);
        }
    }
}
