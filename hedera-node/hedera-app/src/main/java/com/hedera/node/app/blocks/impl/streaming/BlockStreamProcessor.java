// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import com.hedera.hapi.block.PublishStreamRequest;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.data.BlockStreamConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * BlockStreamProcessor is responsible for polling the block stream state manager for new requests and sending them to an active connection.
 * It runs in a separate thread and continuously checks for new requests to process and also handles jump signals to specific blocks.
 */
public class BlockStreamProcessor implements Runnable {
    private static final Logger logger = LogManager.getLogger(BlockStreamProcessor.class);
    private static final int LOOP_DELAY_MS = 10;

    private final BlockStreamStateManager blockStreamStateManager;

    private final AtomicLong jumpTargetBlock = new AtomicLong(-1);

    private final AtomicLong blockNumber = new AtomicLong(-1);
    private int requestIndex = 0;

    private Thread thread;

    /**
     * Constructor for BlockStreamProcessor.
     * @param configProvider The configuration provider to get the BlockStreamConfig.
     * @param blockStreamStateManager The BlockStreamStateManager which manages global in-memory state related to the block stream.
     */
    public BlockStreamProcessor(
            @NonNull ConfigProvider configProvider, @NonNull BlockStreamStateManager blockStreamStateManager) {
        this.blockStreamStateManager = blockStreamStateManager;
        if (configProvider
                .getConfiguration()
                .getConfigData(BlockStreamConfig.class)
                .streamToBlockNodes()) {
            thread = Thread.ofPlatform().name("BlockStreamProcessor").start(this);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                boolean shouldSleep = true;
                // Check if the processor has been signaled to jump to a specific block
                final long targetBlock = jumpTargetBlock.getAndSet(-1); // Check and clear jump signal atomically
                if (targetBlock >= 0) {
                    logger.debug(
                            "[{}] BlockStreamProcessor received jump signal to block {}",
                            Thread.currentThread().getName(),
                            targetBlock);
                    blockNumber.set(targetBlock);
                    requestIndex = 0; // Reset request index for the new block
                    continue; // Restart loop iteration for the new block
                }

                final BlockNodeConnection currentActiveConnection = blockStreamStateManager.getActiveConnection();
                if (currentActiveConnection != null && blockNumber.get() != -1) {
                    // Get the BlockState for streaming to the active connection
                    final BlockState blockState = blockStreamStateManager.getBlockState(blockNumber.get());
                    if (blockState != null && !blockState.requests().isEmpty()) {
                        if (requestIndex < blockState.requests().size()) {
                            logger.trace(
                                    "[{}] Processing block {} for node {}, isComplete: {}, requests: {}, requestIndex: {}",
                                    Thread.currentThread().getName(),
                                    blockNumber,
                                    currentActiveConnection.getNodeConfig().address() + ":"
                                            + currentActiveConnection
                                                    .getNodeConfig()
                                                    .port(),
                                    blockState.isComplete(),
                                    blockState.requests().size(),
                                    requestIndex);
                            PublishStreamRequest publishStreamRequest =
                                    blockState.requests().get(requestIndex);
                            currentActiveConnection.sendRequest(publishStreamRequest);
                            requestIndex++;
                        }

                        // If the requestIndex is greater than the blockState's requests list size and the blockState is
                        // complete then
                        // blockNumber is incremented
                        if (requestIndex >= blockState.requests().size() && blockState.isComplete()) {
                            // Check if there is a higher priority ready connection
                            if (blockStreamStateManager.higherPriorityStarted(currentActiveConnection)) {
                                logger.trace(
                                        "[{}] BlockStreamProcessor higher priority block node chosen {}",
                                        Thread.currentThread().getName(),
                                        blockStreamStateManager
                                                        .getActiveConnection()
                                                        .getNodeConfig()
                                                        .address() + ":"
                                                + blockStreamStateManager
                                                        .getActiveConnection()
                                                        .getNodeConfig()
                                                        .port());
                                blockNumber.set(blockStreamStateManager.getBlockNumber());
                                requestIndex = 0; // Reset request index
                                continue;
                            } else {
                                logger.trace(
                                        "[{}] BlockStreamProcessor incrementing blockNumber to {}",
                                        Thread.currentThread().getName(),
                                        blockNumber.get() + 1);
                                blockNumber.getAndIncrement();
                                requestIndex = 0; // Reset request index for the next block
                            }
                        }

                        if (requestIndex <= blockState.requests().size() && !blockState.isComplete()) {
                            shouldSleep = false; // Don't sleep if there are more requests to process
                        }
                    }
                }

                try {
                    // Sleep for a short duration to avoid busy waiting
                    if (shouldSleep) {
                        Thread.sleep(LOOP_DELAY_MS);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                logger.error(
                        "[{}] BlockStreamProcessor encountered an error: {}",
                        Thread.currentThread().getName(),
                        e.getMessage(),
                        e);
            }
        }
    }

    /**
     * Setting the atomic long will signal the processor to jump to a specific block at the next iteration.
     *
     * @return The jump target block number atomic long.
     */
    public AtomicLong getJumpTargetBlock() {
        return jumpTargetBlock;
    }

    /**
     * Get the block number that is currently being processed.
     * @return The current block number.
     */
    public AtomicLong getBlockNumber() {
        return blockNumber;
    }
}
