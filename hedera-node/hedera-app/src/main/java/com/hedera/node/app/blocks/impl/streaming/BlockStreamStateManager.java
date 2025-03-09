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
    private volatile BlockState currentBlock;
    private final int blockItemBatchSize;

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
     * Opens a new block with the given block number.
     * 
     * @param blockNumber the block number
     * @throws IllegalArgumentException if the block number is negative
     */
    public void openBlock(long blockNumber) {
        if (blockNumber < 0) throw new IllegalArgumentException("Block number must be non-negative");

        currentBlock = BlockState.from(blockNumber);
        blockStates.put(blockNumber, currentBlock);
        logger.info("Started new block in BlockStreamStateManager {}", blockNumber);
    }

    /**
     * Adds a new item to the current block.
     * 
     * @param bytes the item bytes to add
     * @throws IllegalStateException if no block is currently open
     */
    public void addItem(@NonNull Bytes bytes) {
        requireNonNull(bytes, "bytes must not be null");
        
        if (currentBlock == null) {
            throw new IllegalStateException("Received block item before opening block");
        }
        
        currentBlock.itemBytes().add(bytes);
        
        // If we have enough items, create a new request
        if (currentBlock.itemBytes().size() >= blockItemBatchSize) {
            createRequestFromCurrentItems();
        }
    }

    /**
     * Creates a new PublishStreamRequest from the current items in the block.
     */
    private void createRequestFromCurrentItems() {
        if (currentBlock.itemBytes().isEmpty()) {
            return;
        }
        
        List<com.hedera.hapi.block.stream.protoc.BlockItem> protocBlockItems = new ArrayList<>();
        for (Bytes batchItem : currentBlock.itemBytes()) {
            try {
                protocBlockItems.add(
                        com.hedera.hapi.block.stream.protoc.BlockItem.parseFrom(batchItem.toByteArray()));
            } catch (IOException e) {
                logger.error("Failed to parse block item", e);
                throw new RuntimeException(e);
            }
        }

        // Create BlockItemSet by adding all items at once
        BlockItemSet itemSet = BlockItemSet.newBuilder().addAllBlockItems(protocBlockItems).build();
        
        // Create the request and add it to the list
        PublishStreamRequest request = PublishStreamRequest.newBuilder().setBlockItems(itemSet).build();
        currentBlock.requests().add(request);
        
        // Clear the item bytes list
        currentBlock.itemBytes().clear();
    }

    /**
     * Closes the current block and marks it as complete.
     * 
     * @throws IllegalStateException if no block is currently open
     */
    public void closeBlock() {
        if (currentBlock == null) {
            throw new IllegalStateException("Received close block before opening block");
        }
        
        final long blockNumber = currentBlock.blockNumber();
        
        try {
            BlockState block = blockStates.get(blockNumber);
            if (block == null) {
                logger.error("Could not find block state for block {}", blockNumber);
                return;
            }
            
            // Create a final request from any remaining items
            if (!block.itemBytes().isEmpty()) {
                createRequestFromCurrentItems();
            }
            
            // Mark the block as complete
            block.setComplete();
            
            logger.info("Closed block in BlockStreamStateManager {}", blockNumber);
            currentBlock = null;
        } finally {
            // We don't remove the block state here as it might still be needed by the connection manager
        }
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
     * Removes the block state for the given block number.
     * 
     * @param blockNumber the block number
     */
    public void removeBlockState(long blockNumber) {
        blockStates.remove(blockNumber);
    }

    /**
     * Gets all block numbers currently being managed.
     * 
     * @return a list of all block numbers
     */
    public List<Long> getAllBlockNumbers() {
        return new ArrayList<>(blockStates.keySet());
    }
} 