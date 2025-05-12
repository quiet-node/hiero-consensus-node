// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.hedera.hapi.block.PublishStreamRequest;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponse.Acknowledgement;
import com.hedera.hapi.block.PublishStreamResponse.EndOfStream;
import com.hedera.hapi.block.PublishStreamResponse.ResendBlock;
import com.hedera.hapi.block.PublishStreamResponse.SkipBlock;
import com.hedera.node.app.metrics.BlockStreamMetrics;
import com.hedera.node.internal.network.BlockNodeConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.stub.StreamObserver;
import io.helidon.webclient.grpc.GrpcServiceClient;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a single connection to a block node. Each connection is responsible for connecting to configured block nodes
 */
public class BlockNodeConnection implements StreamObserver<PublishStreamResponse> {
    private static final Logger logger = LogManager.getLogger(BlockNodeConnection.class);
    private static final int MAX_END_OF_STREAM_RESTARTS = 3;
    private static final int MAX_END_OF_STREAM_EXP_RETRIES = 10;
    private final ScheduledExecutorService scheduler;

    private final BlockNodeConfig blockNodeConfig;
    private final GrpcServiceClient grpcServiceClient;
    private final BlockNodeConnectionManager blockNodeConnectionManager;
    private final BlockStreamStateManager blockStreamStateManager;
    private final BlockStreamProcessor blockStreamProcessor;
    private BlockStreamMetrics blockStreamMetrics = null;
    private final String connectionDescriptor;

    // Locks and synchronization objects
    private final Object workerLock = new Object();

    // Atomic state variables
    private final AtomicBoolean streamCompletionInProgress = new AtomicBoolean(false);
    private final AtomicInteger endOfStreamImmediateRestarts = new AtomicInteger(0);
    private final AtomicInteger endOfStreamExpBackoffs = new AtomicInteger(0);

    // Volatile connection state
    private volatile StreamObserver<PublishStreamRequest> requestObserver;
    private volatile Thread requestWorker;
    private volatile ConnectionState connectionState;

    /**
     * Enum representing the current state of the connection.
     */
    public enum ConnectionState {
        /**
         * bidi RequestObserver needs to be created.
         */
        UNINITIALIZED,
        /**
         * bidi RequestObserver is established but this connection has not been chosen as the active one (priority based).
         */
        PENDING,
        /**
         * Connection is active. Request Worker Thread is sending PublishStreamRequest's to the block node through async bidi stream.
         */
        ACTIVE
    }

    protected BlockNodeConnection() {
        // Default constructor for NoOpConnection
        this.blockStreamProcessor = null;
        this.blockNodeConfig = null;
        this.grpcServiceClient = null;
        this.blockNodeConnectionManager = null;
        this.blockStreamStateManager = null;
        this.scheduler = null;
        this.connectionDescriptor = null;
    }

    /**
     * Construct a new BlockNodeConnection.
     *
     * @param nodeConfig the configuration for the block node
     * @param blockNodeConnectionManager the connection manager for block node connections
     * @param blockStreamStateManager the block stream state manager for block node connections
     * @param grpcServiceClient the gRPC client to establish the bidirectional streaming to block node connections
     * @param scheduler the scheduler for the connection
     * @param blockStreamMetrics the block stream metrics for block node connections
     */
    public BlockNodeConnection(
            @NonNull final BlockNodeConfig nodeConfig,
            @NonNull final BlockNodeConnectionManager blockNodeConnectionManager,
            @NonNull final BlockStreamStateManager blockStreamStateManager,
            @NonNull final BlockStreamProcessor blockStreamProcessor,
            @NonNull final GrpcServiceClient grpcServiceClient,
            @NonNull final ScheduledExecutorService scheduler,
            @NonNull final BlockStreamMetrics blockStreamMetrics) {
        this.blockNodeConfig = requireNonNull(nodeConfig, "nodeConfig must not be null");
        this.blockNodeConnectionManager =
                requireNonNull(blockNodeConnectionManager, "blockNodeConnectionManager must not be null");
        this.blockStreamStateManager =
                requireNonNull(blockStreamStateManager, "blockStreamStateManager must not be null");
        this.blockStreamProcessor = requireNonNull(blockStreamProcessor, "blockStreamProcessor must not be null");
        this.grpcServiceClient = requireNonNull(grpcServiceClient, "grpcServiceClient must not be null");
        this.scheduler = requireNonNull(scheduler, "scheduler must not be null");
        this.blockStreamMetrics = requireNonNull(blockStreamMetrics, "blockStreamMetrics must not be null");
        this.connectionDescriptor = generateConnectionDescriptor(nodeConfig);
        this.connectionState = ConnectionState.UNINITIALIZED;
    }

    public void createRequestObserver() {
        requestObserver = grpcServiceClient.bidi(blockNodeConnectionManager.getGrpcEndPoint(), this);
    }

    /**
     * @param newState the new state to transition to
     */
    public void updateConnectionState(@NonNull final ConnectionState newState) {
        connectionState = newState;
    }

    /**
     * @return the current state of the connection
     */
    @VisibleForTesting
    ConnectionState getConnectionState() {
        return connectionState;
    }

    private void handleStreamFailure() {
        close();
        blockNodeConnectionManager.handleConnectionError(this);
    }

    private void handleEndOfStreamError() {
        scheduler.schedule(
                () -> {
                    logger.debug(
                            "[{}] Attempting retry after internal error for node {} at block {}",
                            Thread.currentThread().getName(),
                            connectionDescriptor,
                            blockStreamProcessor.getBlockNumber());
                    blockNodeConnectionManager.handleConnectionError(this);
                },
                5,
                TimeUnit.SECONDS);
    }

    private void handleAcknowledgement(@NonNull final Acknowledgement acknowledgement) {
        if (acknowledgement.hasBlockAck()) {
            final var blockAck = acknowledgement.blockAck();
            final var acknowledgedBlockNumber = blockAck.blockNumber();
            final var blockAlreadyExists = blockAck.blockAlreadyExists();
            synchronized (blockStreamProcessor.getBlockNumber()) {
                final long currentBlockStreaming = blockStreamProcessor.getBlockNumber().get();
                final var currentBlockProducing = blockStreamStateManager.getBlockNumber();

                // Update the last verified block by the current connection
                blockNodeConnectionManager.updateLastVerifiedBlock(blockNodeConfig, acknowledgedBlockNumber);

                // Increment the ACK counter metric
                blockStreamMetrics.incrementBlockAckReceivedCount();

                if (currentBlockStreaming == -1) {
                    return;
                }

                if (blockAlreadyExists) {
                    logger.debug(
                            "[{}] Block {} already exists on block node {}",
                            Thread.currentThread().getName(),
                            acknowledgedBlockNumber,
                            connectionDescriptor);
                } else {
                    logger.debug(
                            "[{}] Block {} acknowledgement received from block node {}",
                            Thread.currentThread().getName(),
                            acknowledgedBlockNumber,
                            connectionDescriptor);
                }

                if (currentBlockStreaming < acknowledgedBlockNumber
                        && currentBlockProducing >= acknowledgedBlockNumber) {
                    // We can jump to streaming the acknowledged block number
                    logger.debug(
                            "[{}] Currently streaming Block {} to Block Node {} and acknowledged Block {} - moving streaming ahead to Block {}",
                            Thread.currentThread().getName(),
                            currentBlockStreaming,
                            connectionDescriptor,
                            acknowledgedBlockNumber,
                            acknowledgedBlockNumber + 1L);
                    jumpToBlock(acknowledgedBlockNumber + 1);
                } else if (currentBlockStreaming == acknowledgedBlockNumber
                        && currentBlockProducing == acknowledgedBlockNumber) {
                    // We are already streaming the acknowledged block number
                    logger.debug(
                            "[{}] Currently streaming Block {} to Block Node {} and acknowledged Block {} - (no buffer interaction) moving streaming ahead to Block {}",
                            Thread.currentThread().getName(),
                            currentBlockStreaming,
                            connectionDescriptor,
                            acknowledgedBlockNumber,
                            acknowledgedBlockNumber + 1L);
                    jumpToBlock(acknowledgedBlockNumber + 1);
                } else if (currentBlockStreaming < acknowledgedBlockNumber) {
                    logger.debug(
                            "[{}] Currently producing Block {} to Block Node {} and acknowledged Block {} - moving streaming ahead to Block {}",
                            Thread.currentThread().getName(),
                            currentBlockProducing,
                            connectionDescriptor,
                            acknowledgedBlockNumber,
                            acknowledgedBlockNumber + 1L);
                    jumpToBlock(acknowledgedBlockNumber + 1);
                }
            }
        } else {
            logger.warn("Unknown acknowledgement received: {}", acknowledgement);
        }
    }

    private void handleEndOfStream(@NonNull final EndOfStream endOfStream) {
        final var blockNumber = endOfStream.blockNumber();
        final var responseCode = endOfStream.status();

        logger.debug(
                "[{}] Received EndOfStream from block node {} at block {} with PublishStreamResponseCode {}",
                Thread.currentThread().getName(),
                connectionDescriptor,
                blockNumber,
                responseCode);

        // Always end the stream when we receive an end of stream message
        close();

        switch (responseCode) {
            case STREAM_ITEMS_INTERNAL_ERROR, STREAM_ITEMS_PERSISTENCE_FAILED -> {
                // The block node had an end of stream error and cannot continue processing.
                // We should wait for a short period before attempting to retry
                // to avoid overwhelming the node if it's having issues
                logger.warn(
                        "[{}] Block node {} reported an error at block {}. Will attempt to reestablish the stream later.",
                        Thread.currentThread().getName(),
                        connectionDescriptor,
                        blockNumber);

                if (endOfStreamExpBackoffs.incrementAndGet() <= MAX_END_OF_STREAM_EXP_RETRIES) {
                    handleEndOfStreamError();
                }
            }
            case STREAM_ITEMS_SUCCESS,
                    STREAM_ITEMS_TIMEOUT,
                    STREAM_ITEMS_OUT_OF_ORDER,
                    STREAM_ITEMS_BAD_STATE_PROOF -> {
                // We should restart the stream at the block immediately
                // following the block where the node fell behind.
                final long restartBlockNumber = blockNumber == Long.MAX_VALUE ? 0 : blockNumber + 1;
                logger.warn(
                        "[{}] Block node {} reported status indicating immediate restart should be attempted. Will restart stream at block {}.",
                        Thread.currentThread().getName(),
                        connectionDescriptor,
                        restartBlockNumber);

                if (endOfStreamImmediateRestarts.incrementAndGet() <= MAX_END_OF_STREAM_RESTARTS) {
                    restartStreamAtBlock(restartBlockNumber);
                } else {
                    handleEndOfStreamError();
                }
            }
            case STREAM_ITEMS_BEHIND -> {
                // The block node is behind us, check if we have the last verified block still available in order to
                // restart the stream from there
                final long restartBlockNumber = blockNumber == Long.MAX_VALUE ? 0 : blockNumber + 1;
                if (blockStreamStateManager.getBlockState(restartBlockNumber) != null) {
                    logger.warn(
                            "[{}] Block node {} reported it is behind. Will restart stream at block {}.",
                            Thread.currentThread().getName(),
                            connectionDescriptor,
                            restartBlockNumber);
                    if (endOfStreamImmediateRestarts.incrementAndGet() <= MAX_END_OF_STREAM_RESTARTS) {
                        restartStreamAtBlock(restartBlockNumber);
                    } else {
                        handleEndOfStreamError();
                    }
                } else {
                    // If we don't have the block state, we schedule retry for this connection and establish new one
                    // with different block node
                    logger.warn(
                            "[{}] Block node {} is behind and block state is not available. Closing connection and retrying.",
                            Thread.currentThread().getName(),
                            connectionDescriptor);

                    if (endOfStreamExpBackoffs.incrementAndGet() <= MAX_END_OF_STREAM_EXP_RETRIES) {
                        blockNodeConnectionManager.handleConnectionError(this);
                    }
                }
            }
            case STREAM_ITEMS_UNKNOWN -> {
                // This should never happen, but if it does, we should close the connection
                logger.error(
                        "[{}] Block node {} reported an unknown error at block {}. Closing connection.",
                        Thread.currentThread().getName(),
                        connectionDescriptor,
                        blockNumber);
            }
        }
    }

    private void handleSkipBlock(@NonNull final SkipBlock skipBlock) {
        final var skipBlockNumber = skipBlock.blockNumber();
        final long streamingBlockNumber = blockStreamProcessor.getBlockNumber().get();

        // Only jump if the skip is for the block we are currently processing
        if (skipBlockNumber == streamingBlockNumber) {
            final var nextBlock = skipBlockNumber + 1L;
            logger.debug("Skipping ahead to Block {} because of SkipBlock from {}", nextBlock, connectionDescriptor);
            jumpToBlock(nextBlock); // Now uses signaling instead of thread interruption
        } else {
            logger.warn(
                    "Received SkipBlock for {} but currently processing block {}, ignoring. {}",
                    skipBlockNumber,
                    streamingBlockNumber,
                    connectionDescriptor);
        }
    }

    private void handleResendBlock(@NonNull final ResendBlock resendBlock) {
        final var resendBlockNumber = resendBlock.blockNumber();

        logger.debug(
                "[{}] Received ResendBlock from block node {} for block {}",
                Thread.currentThread().getName(),
                connectionDescriptor,
                resendBlockNumber);

        close();

        logger.debug(
                "[{}] Restarting stream at the next block {} after the last verified one for block node {}",
                Thread.currentThread().getName(),
                resendBlockNumber,
                connectionDescriptor);
        restartStreamAtBlock(resendBlockNumber);
    }

    private String generateConnectionDescriptor(final BlockNodeConfig nodeConfig) {
        return nodeConfig.address() + ":" + nodeConfig.port();
    }

    /**
     * If connection is active sends a request to the block node, otherwise does nothing.
     *
     * @param request the request to send
     */
    public void sendRequest(@NonNull final PublishStreamRequest request) {
        requireNonNull(request);
        if (isActive() && requestObserver != null) {
            requestObserver.onNext(request);
        }
    }

    /**
     * Idempotent operation that closes this connection (if active)
     */
    public void close() {
        updateConnectionState(ConnectionState.UNINITIALIZED);
        logger.debug(
                "[{}] BlockNodeConnection {} ConnectionState: {}",
                Thread.currentThread().getName(),
                connectionDescriptor,
                connectionState);
        closeObserver();
        blockStreamProcessor.getJumpTargetBlock().set(-1L);

        logger.debug(
                "[{}] Closed connection to block node {}",
                Thread.currentThread().getName(),
                connectionDescriptor);
    }

    private void closeObserver() {
        if (requestObserver != null) {
            try {
                logger.debug(
                        "[{}] Closing request observer for block node - requestObserver.onCompleted() {}",
                        Thread.currentThread().getName(),
                        connectionDescriptor);
                streamCompletionInProgress.set(true);
                requestObserver.onCompleted();
            } catch (Exception e) {
                logger.warn("Error while completing request observer", e);
            }
            requestObserver = null;
        }
        stopWorkerThread();
    }

    /**
     * Returns whether the connection is active.
     *
     * @return true if the connection is active, false otherwise
     */
    public boolean isActive() {
        return getConnectionState() == ConnectionState.ACTIVE;
    }

    /**
     * Returns the block node configuration for this connection.
     *
     * @return the block node configuration
     */
    public BlockNodeConfig getNodeConfig() {
        return blockNodeConfig;
    }

    /**
     * Restarts a new stream at a specific block number.
     * This method will establish a new stream and start processing from the specified block number.
     *
     * @param blockNumber the block number to restart at
     */
    public void restartStreamAtBlock(final long blockNumber) {
        logger.debug("Restarting stream at block {} for node {}", blockNumber, connectionDescriptor);

        blockStreamProcessor.getJumpTargetBlock().set(blockNumber);
        blockNodeConnectionManager.scheduleRetry(this, BlockNodeConnectionManager.INITIAL_RETRY_DELAY);

        logger.debug("Stream restarted at block {} for node {}", blockNumber, connectionDescriptor);
    }

    /**
     * Restarts the worker thread at a specific block number without ending the stream.
     * This method will interrupt the current worker thread if it exists,
     * set the new block number and request index, and start a new worker thread.
     * The gRPC stream with the block node is maintained.
     *
     * @param blockNumber the block number to jump to
     */
    public void jumpToBlock(final long blockNumber) {
        logger.debug(
                "[{}] Jumping to block {} for node {}",
                Thread.currentThread().getName(),
                blockNumber,
                connectionDescriptor);
        // Set the target block for the worker loop to pick up

    }

    /**
     * Stops the current worker thread if it exists and waits for it to terminate.
     */
    private void stopWorkerThread() {
        synchronized (workerLock) {
            if (requestWorker != null) {
                requestWorker.interrupt();
                try {
                    requestWorker.join(5000); // Wait up to 5 seconds for thread to stop
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for request worker to stop");
                }
                requestWorker = null;
            }
        }
    }

    @Override
    public void onNext(final PublishStreamResponse response) {
        if (response.hasAcknowledgement()) {
            handleAcknowledgement(response.acknowledgement());
        } else if (response.hasEndStream()) {
            blockStreamMetrics.incrementEndOfStreamCount(response.endStream().status()); // Remove nodeId
            handleEndOfStream(response.endStream());
        } else if (response.hasSkipBlock()) {
            blockStreamMetrics.incrementSkipBlockCount();
            logger.debug(
                    "Received SkipBlock from block node {}  Block #{}",
                    connectionDescriptor,
                    response.skipBlock().blockNumber());
            handleSkipBlock(response.skipBlock());
        } else if (response.hasResendBlock()) {
            blockStreamMetrics.incrementResendBlockCount();
            handleResendBlock(response.resendBlock());
        }
    }

    @Override
    public void onError(final Throwable error) {
        logger.error(
                "[{}] Error on stream from block node {}",
                Thread.currentThread().getName(),
                connectionDescriptor,
                error);
        blockStreamMetrics.incrementOnErrorCount();
        handleStreamFailure();
    }

    @Override
    public void onCompleted() {
        if (!streamCompletionInProgress.get()) {
            logger.debug(
                    "[{}] Stream completed for block node {}",
                    Thread.currentThread().getName(),
                    connectionDescriptor);
            handleStreamFailure();
        } else {
            logger.debug(
                    "[{}] Stream completed for block node {} (stream completion in progress)",
                    Thread.currentThread().getName(),
                    connectionDescriptor);
            streamCompletionInProgress.set(false);
        }
    }

    public ConnectionState getState() {
        return connectionState;
    }
}
