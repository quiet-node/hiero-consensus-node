// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
import com.hedera.node.internal.network.BlockNodeConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a single connection to a block node. Each connection is responsible for connecting to configured block nodes
 */
public class BlockNodeConnection implements StreamObserver<PublishStreamResponse> {
    private static final Logger logger = LogManager.getLogger(BlockNodeConnection.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final BlockNodeConfig node;
    private final BlockNodeConnectionManager manager;
    private final BlockStreamStateManager blockStreamStateManager;
    private final Object channelLock = new Object();
    private volatile ManagedChannel channel;
    private volatile StreamObserver<PublishStreamRequest> requestObserver;

    private final ReentrantLock isActiveLock = new ReentrantLock();
    private volatile boolean isActive = false;

    // Variables for tracking the current block and request index
    private final Object newBlockAvailable = new Object();
    private final Object newRequestAvailable = new Object();
    private long currentBlockNumber = -1;
    private int currentRequestIndex = 0;

    // Worker thread for submitting requests
    private Thread requestWorker;

    public BlockNodeConnection(
            BlockNodeConfig nodeConfig,
            BlockNodeConnectionManager manager,
            BlockStreamStateManager blockStreamStateManager) {
        this.node = nodeConfig;
        this.manager = manager;
        this.blockStreamStateManager = blockStreamStateManager;
        this.channel = createNewChannel();
        logger.info("BlockNodeConnection INITIALIZED");
    }

    private ManagedChannel createNewChannel() {
        return ManagedChannelBuilder.forAddress(node.address(), node.port())
                .usePlaintext() // 🔥🔥 For development only! change to use TLS in production 🔥🔥
                .build();
    }

    public Void establishStream() {
        logger.info("Establishing stream to block node {}:{}", node.address(), node.port());
        synchronized (channelLock) {
            // Ensure any existing channel is properly shutdown
            if (channel != null && !channel.isShutdown()) {
                try {
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while shutting down channel");
                }
            }
            
            // Create new channel and stub
            channel = createNewChannel();
            BlockStreamServiceGrpc.BlockStreamServiceStub stub = BlockStreamServiceGrpc.newStub(channel);
            
            synchronized (isActiveLock) {
                requestObserver = stub.publishBlockStream(this);
                isActive = true;
                startRequestWorker();
            }
        }
        return null;
    }

    private void startRequestWorker() {
        requestWorker = Thread.ofVirtual()
                .name("BlockNodeConnection-RequestWorker-" + node.address() + ":" + node.port())
                .start(this::requestWorkerLoop);
        logger.info("Started request worker thread for block node {}:{}", node.address(), node.port());
    }

    private void requestWorkerLoop() {
        while (isActive) {
            try {
                // Wait for a block to be available if we don't have one yet
                if (currentBlockNumber == -1 || blockStreamStateManager.getBlockState(currentBlockNumber) == null) {
                    logger.info(
                            "[{}] Waiting for new block to be available for node {}:{}",
                            Thread.currentThread().getName(),
                            node.address(),
                            node.port());
                    waitForNewBlock();
                }

                // Get the current block state
                BlockState blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
                if (blockState == null) {
                    logger.info(
                            "[{}] Block {} state is null for node {}:{}, likely due to acknowledgement. Ending stream.",
                            Thread.currentThread().getName(),
                            currentBlockNumber,
                            node.address(),
                            node.port());
                    
                    // End the stream gracefully and schedule reconnect
                    close();
                    scheduleReconnect();
                    break;
                }

                logBlockProcessingInfo(blockState);

                // If there are no requests yet, wait for some to be added
                if (blockState.requests().isEmpty() && !blockState.isComplete()) {
                    logger.info(
                            "[{}] No requests available for block {} on node {}:{}, waiting...",
                            Thread.currentThread().getName(),
                            currentBlockNumber,
                            node.address(),
                            node.port());
                    waitForNewRequests();
                }

                // If we've processed all available requests but the block isn't complete,
                // wait for more requests to be added
                if (needToWaitForMoreRequests(blockState)) {
                    waitForNewRequests();
                }

                // Process any available requests
                processAvailableRequests(blockState);

                // If the block is complete and we've sent all requests, move to the next block
                if (blockState.isComplete()
                        && currentRequestIndex >= blockState.requests().size()) {
                    moveToNextBlock();
                }
            } catch (Exception e) {
                handleGenericException(e);
            }
        }
        logger.info(
                "[{}] Request worker thread exiting for node {}:{}",
                Thread.currentThread().getName(),
                node.address(),
                node.port());
    }

    private void waitForNewBlock() throws InterruptedException {
        synchronized (newBlockAvailable) {
            newBlockAvailable.wait();
        }
    }

    private void waitForNewRequests() throws InterruptedException {
        logger.info(
                "[{}] Waiting for new requests to be available for block {} on node {}:{}, "
                        + "currentRequestIndex: {}, requestsSize: {}",
                Thread.currentThread().getName(),
                currentBlockNumber,
                node.address(),
                node.port(),
                currentRequestIndex,
                blockStreamStateManager.getBlockState(currentBlockNumber) != null
                        ? blockStreamStateManager
                                .getBlockState(currentBlockNumber)
                                .requests()
                                .size()
                        : 0);
        synchronized (newRequestAvailable) {
            newRequestAvailable.wait();
        }
    }

    private void logBlockProcessingInfo(BlockState blockState) {
        logger.info(
                "[{}] Processing block {} for node {}:{}, isComplete: {}, requests: {}",
                Thread.currentThread().getName(),
                currentBlockNumber,
                node.address(),
                node.port(),
                blockState.isComplete(),
                blockState.requests().size());
    }

    private boolean needToWaitForMoreRequests(BlockState blockState) {
        return currentRequestIndex >= blockState.requests().size() && !blockState.isComplete();
    }

    private void processAvailableRequests(BlockState blockState) {
        int requestsSize = blockState.requests().size();
        while (currentRequestIndex < requestsSize) {
            PublishStreamRequest request = blockState.requests().get(currentRequestIndex);
            logger.info(
                    "[{}] Sending request for block {} request index {} to node {}:{}, items: {}",
                    Thread.currentThread().getName(),
                    currentBlockNumber,
                    currentRequestIndex,
                    node.address(),
                    node.port(),
                    request.getBlockItems().getBlockItemsCount());
            sendRequest(request);
            currentRequestIndex++;
        }
    }

    private void moveToNextBlock() {
        logger.info(
                "[{}] Completed sending all requests for block {} to node {}:{}",
                Thread.currentThread().getName(),
                currentBlockNumber,
                node.address(),
                node.port());
        currentBlockNumber++;
        currentRequestIndex = 0;
    }

    private void handleGenericException(Exception e) {
        logger.error(
                "[{}] Error in request worker thread for node {}:{}",
                Thread.currentThread().getName(),
                node.address(),
                node.port(),
                e);
        // End the stream gracefully and schedule reconnect
        close();
        scheduleReconnect();
    }

    private void handleAcknowledgement(PublishStreamResponse.Acknowledgement acknowledgement) {
        if (acknowledgement.hasBlockAck()) {
            var blockAck = acknowledgement.getBlockAck();
            var blockNumber = blockAck.getBlockNumber();
            var blockAlreadyExists = blockAck.getBlockAlreadyExists();
            
            logger.info(
                    "Received acknowledgement for block {} from node {}:{}, blockAlreadyExists: {}",
                    blockNumber,
                    node.address(),
                    node.port(),
                    blockAlreadyExists);

            // Remove all block states up to and including this block number
            blockStreamStateManager.removeBlockStatesUpTo(blockNumber);
        }
    }

    private void handleStreamFailure() {
        synchronized (isActiveLock) {
            isActive = false;
            removeFromActiveConnections(node);
        }
    }

    private void handleEndOfStream(PublishStreamResponse.EndOfStream endOfStream) {
        var blockNumber = endOfStream.getBlockNumber();
        var responseCode = endOfStream.getStatus();
        
        logger.info(
                "[{}] Received EndOfStream from block node {}:{} at block {} with PublishStreamResponseCode {}",
                Thread.currentThread().getName(),
                node.address(),
                node.port(),
                blockNumber,
                responseCode);

        // For all error codes, restart at the last verified block + 1
        var restartBlockNumber = blockNumber + 1;
        logger.info(
                "[{}] Restarting stream at block {} due to {} for node {}:{}",
                Thread.currentThread().getName(),
                restartBlockNumber,
                responseCode,
                node.address(),
                node.port());
        endStreamAndRestartAtBlock(restartBlockNumber);
    }

    private void removeFromActiveConnections(BlockNodeConfig node) {
        manager.handleConnectionError(node);
    }

    /**
     * If connection is active sends a request to the block node, otherwise does nothing.
     *
     * @param request the request to send
     */
    public void sendRequest(@NonNull final PublishStreamRequest request) {
        requireNonNull(request);
        synchronized (isActiveLock) {
            if (isActive) {
                requestObserver.onNext(request);
            }
        }
    }

    private void scheduleReconnect() {
        manager.scheduleReconnect(this);
    }

    /**
     * If connection is active it closes it, otherwise does nothing.
     */
    public void close() {
        synchronized (isActiveLock) {
            isActive = false;
        }

        synchronized (channelLock) {
            if (requestObserver != null) {
                try {
                    requestObserver.onCompleted();
                } catch (Exception e) {
                    logger.warn("Error while completing request observer", e);
                }
                requestObserver = null;
            }

            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while shutting down channel during close");
                } finally {
                    channel = null;
                }
            }
        }

        logger.info("Closed connection to block node {}:{}", node.address(), node.port());
    }

    /**
     * Returns whether the connection is active.
     *
     * @return true if the connection is active, false otherwise
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * Returns the block node configuration this connection.
     *
     * @return the block node configuration
     */
    public BlockNodeConfig getNodeConfig() {
        return node;
    }

    /**
     * Gets the current block number being processed.
     *
     * @return the current block number
     */
    public long getCurrentBlockNumber() {
        return currentBlockNumber;
    }

    /**
     * Gets the current request index being processed.
     *
     * @return the current request index
     */
    public int getCurrentRequestIndex() {
        return currentRequestIndex;
    }

    public ReentrantLock getIsActiveLock() {
        return isActiveLock;
    }

    public void notifyNewRequestAvailable() {
        synchronized (newRequestAvailable) {
            BlockState blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
            if (blockState != null) {
                logger.info(
                        "Notifying of new request available for node {}:{} - block: {}, requests: {}, isComplete: {}",
                        node.address(),
                        node.port(),
                        currentBlockNumber,
                        blockState.requests().size(),
                        blockState.isComplete());
            } else {
                logger.info(
                        "Notifying of new request available for node {}:{} - block: {} (state not found)",
                        node.address(),
                        node.port(),
                        currentBlockNumber);
            }
            newRequestAvailable.notify();
        }
    }

    public void notifyNewBlockAvailable() {
        synchronized (newBlockAvailable) {
            newBlockAvailable.notify();
        }
    }

    public void setCurrentBlockNumber(long blockNumber) {
        currentBlockNumber = blockNumber;
        currentRequestIndex = 0; // Reset the request index when setting a new block
        logger.info(
                "Set current block number to {} for node {}:{}, reset request index to 0",
                blockNumber,
                node.address(),
                node.port());
    }

    /**
     * Restarts the worker thread at a specific block number without ending the stream.
     * This method will interrupt the current worker thread if it exists,
     * set the new block number and request index, and start a new worker thread.
     * The gRPC stream with the block node is maintained.
     *
     * @param blockNumber the block number to jump to
     */
    public void jumpToBlock(long blockNumber) {
        logger.info(
                "Jumping to block {} for node {}:{} without ending stream",
                blockNumber,
                node.address(),
                node.port());

        stopWorkerThread();
        setCurrentBlockNumber(blockNumber);
        startRequestWorker();
        notifyNewBlockAvailable();

        logger.info(
                "Worker thread restarted and jumped to block {} for node {}:{}",
                blockNumber,
                node.address(),
                node.port());
    }

    /**
     * Ends the current stream and restarts a new stream at a specific block number.
     * This method will close the current connection, establish a new stream,
     * and start processing from the specified block number.
     *
     * @param blockNumber the block number to restart at
     */
    public void endStreamAndRestartAtBlock(long blockNumber) {
        logger.info(
                "Ending stream and restarting at block {} for node {}:{}",
                blockNumber,
                node.address(),
                node.port());

        // Close the current connection and clean up resources
        close();

        // Set the new block number
        setCurrentBlockNumber(blockNumber);

        // Establish a new stream
        establishStream();

        logger.info(
                "Stream ended and restarted at block {} for node {}:{}",
                blockNumber,
                node.address(),
                node.port());
    }

    /**
     * Restarts the stream and worker thread at a specific block number.
     * This method will interrupt the current worker thread if it exists,
     * set the new block number and request index, and start a new worker thread.
     *
     * @param blockNumber the block number to restart at
     */
    public void restartStreamAtBlock(long blockNumber) {
        logger.info("Restarting worker thread for node {}:{} at block {}", node.address(), node.port(), blockNumber);

        stopWorkerThread();
        setCurrentBlockNumber(blockNumber);
        startRequestWorker();
        notifyNewBlockAvailable();

        logger.info("Worker thread restarted for node {}:{} at block {}", node.address(), node.port(), blockNumber);
    }

    /**
     * Stops the current worker thread if it exists and waits for it to terminate.
     */
    private void stopWorkerThread() {
        // Stop the current worker thread if it exists
        if (requestWorker != null && requestWorker.isAlive()) {
            requestWorker.interrupt();
            try {
                // Wait for the thread to terminate
                requestWorker.join(1000);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for worker thread to terminate", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void onNext(PublishStreamResponse response) {
        if (response.hasAcknowledgement()) {
            handleAcknowledgement(response.getAcknowledgement());
        } else if (response.hasEndStream()) {
            handleEndOfStream(response.getEndStream());
        } else if (response.hasSkipBlock()) {
            logger.info(
                    "Received SkipBlock from Block Node {}:{}  Block #{}",
                    node.address(),
                    node.port(),
                    response.getSkipBlock().getBlockNumber());
        } else if (response.hasResendBlock()) {
            logger.info(
                    "Received ResendBlock from Block Node {}:{}  Block #{}",
                    node.address(),
                    node.port(),
                    response.getResendBlock().getBlockNumber());
        }
    }

    @Override
    public void onError(Throwable error) {
        logger.error(
                "[{}] Error on stream from block node {}:{}",
                Thread.currentThread().getName(),
                node.address(),
                node.port(),
                error);

        synchronized (channelLock) {
            // Shutdown the existing channel
            if (channel != null && !channel.isShutdown()) {
                try {
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while shutting down channel after error");
                }
            }
            // Clear the requestObserver since it's no longer valid
            requestObserver = null;
        }

        // Notify connection manager of the error
        manager.handleConnectionError(node);
    }

    @Override
    public void onCompleted() {
        logger.info("Stream completed for block node {}:{}", node.address(), node.port());
        handleStreamFailure();
    }
}
