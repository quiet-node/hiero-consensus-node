// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.node.internal.network.BlockNodeConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a single connection to a block node. Each connection is responsible for connecting to configured block nodes
 */
public class BlockNodeConnection {
    private static final Logger logger = LogManager.getLogger(BlockNodeConnection.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final BlockNodeConfig node;
    private final ManagedChannel channel;
    private final BlockNodeConnectionManager manager;
    private StreamObserver<PublishStreamRequest> requestObserver;
    private volatile boolean isActive = true;

    public BlockNodeConnection(BlockNodeConfig nodeConfig, BlockNodeConnectionManager manager) {
        this.node = nodeConfig;
        this.manager = manager;
        this.channel = ManagedChannelBuilder.forAddress(nodeConfig.address(), nodeConfig.port())
                .usePlaintext() // For development; use TLS in production
                .build();
        establishStream();
        logger.info("BlockNodeConnection INITIALIZED");
    }

    private void establishStream() {
        BlockStreamServiceGrpc.BlockStreamServiceStub stub = BlockStreamServiceGrpc.newStub(channel);

        requestObserver = stub.publishBlockStream(new StreamObserver<PublishStreamResponse>() {
            @Override
            public void onNext(PublishStreamResponse response) {
                logger.info("Response {}", response);
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
    }

    private void handleAcknowledgement(PublishStreamResponse.Acknowledgement acknowledgement) {
        if (acknowledgement.hasBlockAck()) {
            logger.info(
                    "Block acknowledgment received for a full block: {}",
                    acknowledgement.getBlockAck().getBlockNumber());
        }
    }

    private void handleEndOfStream(PublishStreamResponse.EndOfStream endOfStream) {
        logger.info("Error returned from block node at block number {}: {}", endOfStream.getBlockNumber(), endOfStream);
    }

    private void removeFromActiveConnections(BlockNodeConfig node) {
        manager.handleConnectionError(node);
    }

    public void handleStreamFailure() {
        isActive = false;
        removeFromActiveConnections(node);
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
            scheduler.shutdown();
            // Shutdown the channel gracefully
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Channel shutdown interrupted", e);
                Thread.currentThread().interrupt();
            } finally {
                if (!channel.isShutdown()) {
                    channel.shutdownNow();
                }
            }
        }
    }

    public boolean isActive() {
        return isActive;
    }

    public BlockNodeConfig getNodeConfig() {
        return node;
    }
}
