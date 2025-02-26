// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.node.internal.network.BlockNodeConfig;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.helidon.webclient.grpc.GrpcServiceClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a single connection to a block node. Each connection is responsible for connecting to configured block nodes
 */
public class BlockNodeConnection {
    private static final Logger logger = LogManager.getLogger(BlockNodeConnection.class);
    private static final long RETRY_BACKOFF_MULTIPLIER = 2;

    private final BlockNodeConfig node;
    private final GrpcServiceClient grpcServiceClient;
    private final BlockNodeConnectionManager manager;
    private final ExecutorService retryExecutor;
    private StreamObserver<PublishStreamRequest> requestObserver;
    private volatile boolean isActive = true;

    public BlockNodeConnection(
            BlockNodeConfig nodeConfig,
            GrpcServiceClient grpcServiceClient,
            BlockNodeConnectionManager manager,
            ExecutorService retryExecutor) {
        this.node = nodeConfig;
        this.grpcServiceClient = grpcServiceClient;
        this.manager = manager;
        this.retryExecutor = retryExecutor;
        logger.info("BlockNodeConnection INITIALIZED");
    }

    public Void establishStream() {
        requestObserver =
                grpcServiceClient.bidi(manager.getGrpcEndPoint(), new StreamObserver<PublishStreamResponse>() {
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
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("Stream completed for block node {}:{}", node.address(), node.port());
                        handleStreamFailure();
                    }
                });

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
        manager.handleConnectionError(node);
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

    public void sendRequest(PublishStreamRequest request) {
        if (isActive) {
            requestObserver.onNext(request);
        }
    }

    public void close() {
        if (isActive) {
            isActive = false;
            requestObserver.onCompleted();
            retryExecutor.shutdown();
        }
    }

    public boolean isActive() {
        return isActive;
    }

    public BlockNodeConfig getNodeConfig() {
        return node;
    }

    public <T> void retry(Supplier<T> action, Duration initialDelay, int maxAttempts) throws Exception {
        int attempts = 0;
        Duration delay = initialDelay;

        while (attempts < maxAttempts) {
            try {
                action.get();
                return;
            } catch (Exception e) {
                attempts++;
                if (attempts >= maxAttempts) {
                    throw new Exception("Max retry attempts reached", e);
                }
                logger.info("Failed to execute action, retrying in {} ms", delay.toMillis());
                Thread.sleep(delay.toMillis());
                delay = delay.multipliedBy(RETRY_BACKOFF_MULTIPLIER);
            }
        }
    }
}
