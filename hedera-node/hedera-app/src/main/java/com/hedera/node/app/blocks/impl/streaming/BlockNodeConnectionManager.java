// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.protoc.BlockItemSet;
import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.data.BlockStreamConfig;
import com.hedera.node.internal.network.BlockNodeConfig;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.grpc.GrpcClient;
import io.helidon.webclient.grpc.GrpcClientMethodDescriptor;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.grpc.GrpcServiceClient;
import io.helidon.webclient.grpc.GrpcServiceDescriptor;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages connections to block nodes, connection lifecycle and node selection.
 */
public class BlockNodeConnectionManager {
    private static final Logger logger = LogManager.getLogger(BlockNodeConnectionManager.class);
    private static final String GRPC_END_POINT =
            BlockStreamServiceGrpc.getPublishBlockStreamMethod().getBareMethodName();

    private final Map<BlockNodeConfig, BlockNodeConnection> activeConnections;
    private BlockNodeConfigExtractor blockNodeConfigurations;
    private final BlockStreamStateManager blockStreamStateManager;

    private final Object connectionLock = new Object();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService streamingExecutor = Executors.newSingleThreadExecutor();

    /**
     * Creates a new BlockNodeConnectionManager with the given configuration from disk.
     * @param configProvider the configuration provider
     * @param blockStreamStateManager the block stream state manager
     */
    public BlockNodeConnectionManager(
            @NonNull final ConfigProvider configProvider,
            @NonNull final BlockStreamStateManager blockStreamStateManager) {
        requireNonNull(configProvider);
        requireNonNull(blockStreamStateManager);
        this.activeConnections = new ConcurrentHashMap<>();
        this.blockStreamStateManager = blockStreamStateManager;

        final var blockStreamConfig = configProvider.getConfiguration().getConfigData(BlockStreamConfig.class);
        if (!blockStreamConfig.streamToBlockNodes()) {
            return;
        }
        this.blockNodeConfigurations = new BlockNodeConfigExtractor(blockStreamConfig.blockNodeConnectionFileDir());
    }

    /**
     * Attempts to establish connections to block nodes based on priority and configuration.
     */
    private void establishConnections() {
        logger.info("Establishing connections to block nodes");

        List<BlockNodeConfig> availableNodes = blockNodeConfigurations.getAllNodes().stream()
                .filter(node -> !activeConnections.containsKey(node))
                .toList();

        availableNodes.forEach(this::connectToNode);
    }

    private void connectToNode(@NonNull BlockNodeConfig node) {
        logger.info("Connecting to block node {}:{}", node.address(), node.port());
        try {
            GrpcClient client = GrpcClient.builder()
                    .tls(Tls.builder().enabled(false).build())
                    .baseUri(new URI("http://" + node.address() + ":" + node.port()))
                    .protocolConfig(GrpcClientProtocolConfig.builder()
                            .abortPollTimeExpired(false)
                            .build())
                    .keepAlive(true)
                    .build();

            GrpcServiceClient grpcServiceClient = client.serviceClient(GrpcServiceDescriptor.builder()
                    .serviceName(BlockStreamServiceGrpc.SERVICE_NAME)
                    .putMethod(
                            GRPC_END_POINT,
                            GrpcClientMethodDescriptor.bidirectional(
                                            BlockStreamServiceGrpc.SERVICE_NAME, GRPC_END_POINT)
                                    .requestType(PublishStreamRequest.class)
                                    .responseType(PublishStreamResponse.class)
                                    .build())
                    .build());

            BlockNodeConnection connection = new BlockNodeConnection(node, grpcServiceClient, this, blockStreamStateManager);
            synchronized (connectionLock) {
                connection.getIsActiveLock().lock();
                if (connection.isActive()) {
                    activeConnections.put(node, connection);
                }
                connection.getIsActiveLock().unlock();
            }
            logger.info("Successfully connected to block node {}:{}", node.address(), node.port());
        } catch (URISyntaxException | RuntimeException e) {
            logger.error("Failed to connect to block node {}:{}", node.address(), node.port(), e);
        }
    }

    private synchronized void disconnectFromNode(@NonNull BlockNodeConfig node) {
        synchronized (connectionLock) {
            BlockNodeConnection connection = activeConnections.remove(node);
            if (connection != null) {
                connection.close();
                logger.info("Disconnected from block node {}:{}", node.address(), node.port());
            }
        }
    }

    /**
     * Handles connection errors from a BlockNodeConnection by removing the failed connection
     * and initiating the reconnection process.
     *
     * @param node the node configuration for the failed connection
     */
    public void handleConnectionError(@NonNull BlockNodeConfig node) {
        synchronized (connectionLock) {
            activeConnections.remove(node); // Remove the failed connection
        }
    }

    /**
     * Shuts down the connection manager, closing all active connections.
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            boolean awaitTermination = streamingExecutor.awaitTermination(10, TimeUnit.SECONDS);
            if (!awaitTermination) {
                logger.error("Failed to shut down streaming executor within 10 seconds");
            } else {
                logger.info("Successfully shut down streaming executor");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (BlockNodeConfig node : new ArrayList<>(activeConnections.keySet())) {
            disconnectFromNode(node);
        }
    }

    /**
     * Waits for at least one connection to be established.
     *
     * @param timeout the maximum time to wait
     * @return true if at least one connection was established, false if the timeout elapsed before any connections were established
     */
    public boolean waitForConnection(Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        establishConnections();

        scheduler.scheduleAtFixedRate(
                () -> {
                    if (!activeConnections.isEmpty()) {
                        future.complete(true);
                    } else if (Instant.now().isAfter(Instant.now().plus(timeout))) {
                        future.complete(false);
                    }
                },
                0,
                1,
                TimeUnit.SECONDS);

        return future.join();
    }

    /**
     * Returns the gRPC endpoint for the block stream service.
     *
     * @return the gRPC endpoint
     */
    public String getGrpcEndPoint() {
        return GRPC_END_POINT;
    }

    public void openBlock(long blockNumber) {
        blockStreamStateManager.openBlock(blockNumber);
        synchronized (connectionLock) {
            for (BlockNodeConnection blockNodeConnection : activeConnections.values()) {
                if (blockNodeConnection.getCurrentBlockNumber() == -1) {
                    blockNodeConnection.setCurrentBlockNumber(blockNumber);
                }
                blockNodeConnection.notifyNewBlockAvailable();
            }
        }
    }

    public void notifyConnectionsOfNewRequest() {
        synchronized (connectionLock) {
            for (BlockNodeConnection blockNodeConnection : activeConnections.values()) {
                blockNodeConnection.notifyNewRequestAvailable();
            }
        }
    }


}
