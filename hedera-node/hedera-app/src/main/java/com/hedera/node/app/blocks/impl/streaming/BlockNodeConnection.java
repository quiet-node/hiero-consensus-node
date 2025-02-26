// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.node.internal.network.BlockNodeConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.helidon.webclient.grpc.GrpcServiceClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a single connection to a block node. Each connection is responsible for connecting to configured block nodes.
 * It is also responsible for retrying with exponential backoff if the connection fails.
 */
public class BlockNodeConnection {
    private static final Logger logger = LogManager.getLogger(BlockNodeConnection.class);
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(1);
    private static final long RETRY_BACKOFF_MULTIPLIER = 2;

    private final BlockNodeConfig node;
    private final GrpcServiceClient grpcServiceClient;
    private final BlockNodeConnectionManager blockNodeConnectionManager;
    private final ExecutorService retryExecutor;
    private StreamObserver<PublishStreamRequest> requestObserver;
    private volatile boolean isActive = false;

    /**
     * Construct a new BlockNodeConnection.
     *
     * @param nodeConfig the configuration for the block node
     * @param grpcServiceClient the gRPC service client
     * @param blockNodeConnectionManager the connection manager for block node connections
     * @param retryExecutor the executor for retrying this connection
     */
    public BlockNodeConnection(
            @NonNull final BlockNodeConfig nodeConfig,
            @NonNull final GrpcServiceClient grpcServiceClient,
            @NonNull final BlockNodeConnectionManager blockNodeConnectionManager,
            @NonNull final ExecutorService retryExecutor) {
        this.node = requireNonNull(nodeConfig, "nodeConfig must not be null");
        this.grpcServiceClient = requireNonNull(grpcServiceClient, "grpcServiceClient must not be null");
        this.blockNodeConnectionManager =
                requireNonNull(blockNodeConnectionManager, "blockNodeConnectionManager must not be null");
        this.retryExecutor = requireNonNull(retryExecutor, "retryExecutor must not be null");
        logger.info("BlockNodeConnection INITIALIZED");
    }

    public Void establishStream() {
        requestObserver = grpcServiceClient.bidi(
                blockNodeConnectionManager.getGrpcEndPoint(), new StreamObserver<PublishStreamResponse>() {
                    @Override
                    public void onNext(PublishStreamResponse response) {
                        if (response.hasAcknowledgement()) {
                            handleAcknowledgement(response.getAcknowledgement());
                        } else if (response.hasStatus()) {
                            handleEndOfStream(response.getStatus());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        Status status = Status.fromThrowable(t);
                        logger.error("Error in block node stream {}:{}: {}", node.address(), node.port(), status, t);
                        handleStreamFailure();
                        scheduleReconnect();
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("Stream completed for block node {}:{}", node.address(), node.port());
                        handleStreamFailure();
                    }
                });

        isActive = true;
        return null;
    }

    private void handleAcknowledgement(PublishStreamResponse.Acknowledgement acknowledgement) {
        if (acknowledgement.hasBlockAck()) {
            logger.info("Block acknowledgment received for a full block: {}", acknowledgement.getBlockAck());
        } else if (acknowledgement.hasItemAck()) {
            logger.info("Item acknowledgement received for a batch of block items: {}", acknowledgement.getItemAck());
        }
    }

    private void handleStreamFailure() {
        isActive = false;
        removeFromActiveConnections(node);
    }

    private void handleEndOfStream(PublishStreamResponse.EndOfStream endOfStream) {
        logger.info("Error returned from block node at block number {}: {}", endOfStream.getBlockNumber(), endOfStream);
    }

    private void removeFromActiveConnections(BlockNodeConfig node) {
        blockNodeConnectionManager.handleConnectionError(node);
    }

    private void scheduleReconnect() {
        retryExecutor.execute(() -> {
            try {
                retry(this::establishStream, INITIAL_RETRY_DELAY, MAX_RETRY_ATTEMPTS);
            } catch (Exception e) {
                logger.error("Failed to re-establish stream to block node {}:{}: {}", node.address(), node.port(), e);
            }
        });
    }

    /**
     * If connection is active sends a request to the block node, otherwise does nothing.
     *
     * @param request the request to send
     */
    public void sendRequest(@NonNull final PublishStreamRequest request) {
        if (isActive) {
            requireNonNull(request);
            requestObserver.onNext(request);
        }
    }

    /**
     * If connection is active it closes it, otherwise does nothing.
     */
    public void close() {
        if (isActive) {
            isActive = false;
            requestObserver.onCompleted();
            retryExecutor.shutdown();
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
     * Retries the given action with exponential backoff.
     *
     * @param action the action to retry
     * @param initialDelay the initial delay before the first retry
     * @param maxAttempts the maximum number of attempts
     * @param <T> the return type of the action
     * @throws Exception if the action fails after the maximum number of attempts
     */
    public <T> void retry(@NonNull final Supplier<T> action, @NonNull final Duration initialDelay, int maxAttempts)
            throws Exception {
        requireNonNull(action);
        requireNonNull(initialDelay);

        int attempts = 0;
        Duration delay = initialDelay;

        while (attempts < maxAttempts) {
            try {
                logger.info("Retrying in {} ms", delay.toMillis());
                Thread.sleep(delay.toMillis());
                action.get();
                return;
            } catch (Exception e) {
                attempts++;
                if (attempts >= maxAttempts) {
                    throw new Exception("Max retry attempts reached", e);
                }
                delay = delay.multipliedBy(RETRY_BACKOFF_MULTIPLIER);
            }
        }
    }
}
