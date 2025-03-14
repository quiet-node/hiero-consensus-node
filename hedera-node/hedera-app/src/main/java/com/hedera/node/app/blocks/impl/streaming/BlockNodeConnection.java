// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
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
    private final ManagedChannel channel;
    private final BlockNodeConnectionManager manager;
    private final BlockStreamStateManager blockStreamStateManager;
    private StreamObserver<PublishStreamRequest> requestObserver;

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
        this.channel = ManagedChannelBuilder.forAddress(nodeConfig.address(), nodeConfig.port())
                .usePlaintext() // 🔥🔥 For development only! change to use TLS in production 🔥🔥
                .build();
        logger.info("BlockNodeConnection INITIALIZED");
    }

    public Void establishStream() {
        logger.info("Establishing stream to block node {}:{}", node.address(), node.port());
        BlockStreamServiceGrpc.BlockStreamServiceStub stub = BlockStreamServiceGrpc.newStub(channel);
        synchronized (isActiveLock) {
            requestObserver = stub.publishBlockStream(this);
            isActive = true;
            startRequestWorker();
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
                    continue;
                }

                // Get the current block state
                BlockState blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
                if (blockState == null) {
                    logger.info(
                            "[{}] Block state is null for block {} on node {}:{}",
                            Thread.currentThread().getName(),
                            currentBlockNumber,
                            node.address(),
                            node.port());
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
            } catch (InterruptedException e) {
                handleInterruptedException(e);
                break;
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

    private void handleInterruptedException(InterruptedException e) {
        if (isActive) {
            logger.error(
                    "[{}] Request worker thread interrupted for node {}:{}",
                    Thread.currentThread().getName(),
                    node.address(),
                    node.port(),
                    e);
        }
        Thread.currentThread().interrupt();
    }

    private void handleGenericException(Exception e) {
        logger.error(
                "[{}] Error in request worker thread for node {}:{}",
                Thread.currentThread().getName(),
                node.address(),
                node.port(),
                e);
        // If we get an IndexOutOfBoundsException, reset the request index to a safe value
        if (e instanceof IndexOutOfBoundsException) {
            BlockState blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
            if (blockState != null) {
                int safeIndex = Math.min(blockState.requests().size(), currentRequestIndex);
                logger.info(
                        "[{}] Resetting request index from {} to {} for block {}",
                        Thread.currentThread().getName(),
                        currentRequestIndex,
                        safeIndex,
                        currentBlockNumber);
                currentRequestIndex = safeIndex;
            }
        }
    }

    private void handleAcknowledgement(PublishStreamResponse.Acknowledgement acknowledgement) {
        if (acknowledgement.hasBlockAck()) {
            logger.info(
                    "Block acknowledgment received for a full block: {}",
                    acknowledgement.getBlockAck().getBlockNumber());
        }
    }

    private void handleStreamFailure() {
        synchronized (isActiveLock) {
            isActive = false;
            removeFromActiveConnections(node);
        }
    }

    private void handleEndOfStream(PublishStreamResponse.EndOfStream endOfStream) {
        logger.info("Error returned from block node at block number {}: {}", endOfStream.getBlockNumber(), endOfStream);
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
            if (isActive) {
                requestObserver.onCompleted();
                scheduler.shutdown();
                isActive = false;
                try {
                    if (requestWorker != null) {
                        requestWorker.interrupt();
                        requestWorker.join(1000);
                    }
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.error("BlockNodeConnection interrupted {}:{}", node.address(), node.port(), e);
                    Thread.currentThread().interrupt();
                } finally {
                    if (!channel.isShutdown()) {
                        channel.shutdownNow();
                    }
                }
            }
            logger.info("Closed connection to block node {}:{}", node.address(), node.port());
        }
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

    public Object getNewBlockAvailable() {
        return newBlockAvailable;
    }

    public Object getNewRequestAvailable() {
        return newRequestAvailable;
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
     * Restarts the worker thread at a specific block number.
     * This method will interrupt the current worker thread if it exists,
     * set the new block number and request index, and start a new worker thread.
     *
     * @param blockNumber the block number to restart at
     */
    public void restartWorkerAtBlock(long blockNumber) {
        logger.info("Restarting worker thread for node {}:{} at block {}", node.address(), node.port(), blockNumber);

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

        // Set the new block number and reset request index
        setCurrentBlockNumber(blockNumber);

        // Start a new worker thread
        startRequestWorker();

        // Notify that a new block is available to process
        notifyNewBlockAvailable();

        logger.info("Worker thread restarted for node {}:{} at block {}", node.address(), node.port(), blockNumber);
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
    public void onError(Throwable throwable) {
        Status status = Status.fromThrowable(throwable);
        logger.error(
                "Error in block node stream {}:{}: {} {}", node.address(), node.port(), status, throwable.getMessage());
        handleStreamFailure();
        scheduleReconnect();
    }

    @Override
    public void onCompleted() {
        logger.info("Stream completed for block node {}:{}", node.address(), node.port());
        handleStreamFailure();
    }
}
