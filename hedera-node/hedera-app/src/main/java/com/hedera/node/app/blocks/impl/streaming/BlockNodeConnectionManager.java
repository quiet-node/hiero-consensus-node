// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.hedera.hapi.block.PublishStreamRequest;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.node.app.metrics.BlockStreamMetrics;
import com.hedera.node.internal.network.BlockNodeConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.grpc.GrpcClient;
import io.helidon.webclient.grpc.GrpcClientMethodDescriptor;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.grpc.GrpcServiceClient;
import io.helidon.webclient.grpc.GrpcServiceDescriptor;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages connections to block nodes, connection lifecycle and node selection.
 * It is also responsible for retrying with exponential backoff if a connection fails.
 */
public class BlockNodeConnectionManager {
    public static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(5);
    private static final Logger logger = LogManager.getLogger(BlockNodeConnectionManager.class);
    private static final long RETRY_BACKOFF_MULTIPLIER = 2;
    private static final String GRPC_END_POINT =
            BlockStreamServiceGrpc.getPublishBlockStreamMethod().getBareMethodName();
    // Maximum retry delay to prevent excessively long waits
    private static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(1);

    // Add a random number generator for retry jitter
    private final Random random = new Random();

    private final Map<BlockNodeConfig, BlockNodeConnection> connectionsInRetry;
    private final Map<BlockNodeConfig, Long> lastVerifiedBlockPerConnection;

    private final BlockNodeConfigExtractor blockNodeConfigurations;
    private final BlockStreamStateManager blockStreamStateManager;

    private final ReentrantLock connectionLock = new ReentrantLock();
    private final Condition connectionEstablishedCondition = connectionLock.newCondition();
    private final ScheduledExecutorService connectionExecutor = Executors.newScheduledThreadPool(1);

    private BlockNodeConnection activeConnection;
    private BlockNodeConnection highestPriorityReadyConnection;
    private final BlockStreamMetrics blockStreamMetrics;

    /**
     * Creates a new BlockNodeConnectionManager with the given configuration from disk.
     * @param blockNodeConfigExtractor the block node configuration extractor
     * @param blockStreamStateManager the block stream state manager
     */
    public BlockNodeConnectionManager(
            @NonNull final BlockNodeConfigExtractor blockNodeConfigExtractor,
            @NonNull final BlockStreamStateManager blockStreamStateManager,
            @NonNull final BlockStreamMetrics blockStreamMetrics) {
        this.blockNodeConfigurations =
                requireNonNull(blockNodeConfigExtractor, "blockNodeConfigExtractor must not be null");
        this.blockStreamStateManager =
                requireNonNull(blockStreamStateManager, "blockStreamStateManager must not be null");

        this.connectionsInRetry = new ConcurrentHashMap<>();
        this.lastVerifiedBlockPerConnection = new ConcurrentHashMap<>();
        this.blockStreamMetrics = blockStreamMetrics;
    }

    /**
     * @return the gRPC endpoint for publish block stream
     */
    public String getGrpcEndPoint() {
        return GRPC_END_POINT;
    }

    private GrpcServiceClient createNewGrpcClient(@NonNull BlockNodeConfig node) {
        final GrpcClient client = GrpcClient.builder()
                .tls(Tls.builder().enabled(false).build())
                .baseUri("http://" + node.address() + ":" + node.port())
                .protocolConfig(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(Duration.ofSeconds(30))
                        .build())
                .keepAlive(true)
                .build();

        return client.serviceClient(GrpcServiceDescriptor.builder()
                .serviceName(BlockStreamServiceGrpc.SERVICE_NAME)
                .putMethod(
                        GRPC_END_POINT,
                        GrpcClientMethodDescriptor.bidirectional(BlockStreamServiceGrpc.SERVICE_NAME, GRPC_END_POINT)
                                .requestType(PublishStreamRequest.class)
                                .responseType(PublishStreamResponse.class)
                                .marshallerSupplier(new RequestResponseMarshaller.Supplier())
                                .build())
                .build());
    }

    /**
     * Handles connection errors reported by an active BlockNodeConnection.
     * Schedules a retry for the failed connection and attempts to select a new active node.
     *
     * @param connection the connection that received the error
     */
    public void handleConnectionError(@NonNull final BlockNodeConnection connection) {
        connectionLock.lock();
        try {
            logger.warn("Handling connection error for {}", blockNodeName(connection.getNodeConfig()));
            // Schedule retry for the failed connection (will use INITIAL_RETRY_DELAY)
            scheduleRetry(connection, INITIAL_RETRY_DELAY);
            // Immediately try to find and connect to the next available node
            selectBlockNodeForStreaming();
        } finally {
            connectionLock.unlock();
        }
    }

    public void forgetConnection(@NonNull final BlockNodeConnection connection) {
        synchronized (connectionLock) {
            final BlockNodeConfig nodeConfig = connection.getNodeConfig();
            logger.warn(
                    "Connection for {} block node has exceeded the maximum number of end of stream retries.",
                    blockNodeName(nodeConfig));
            connectionsInRetry.remove(nodeConfig);

            // Remove the connection from the blockNodeConfigurations list, so that establishConnection() doesn't try to
            // connect to it again
            blockNodeConfigurations.getAllNodes().remove(nodeConfig);
        }
    }

    /**
     * Schedules a connection attempt (or retry) for the given Block Node connection
     * after the specified delay. Handles adding/removing the connection from the retry map.
     *
     * @param connection the connection to schedule a retry for
     * @param initialDelay the delay before the first attempt in this sequence executes
     */
    public void scheduleRetry(
            @NonNull final BlockNodeConnection connection, @NonNull final Duration initialDelay) {
        requireNonNull(connection);
        requireNonNull(initialDelay);
        final var nodeConfig = connection.getNodeConfig();

        // Check if already genuinely marked for retry scheduling/running
        // Note: isRetrying checks connectionsInRetry map
        if (isRetrying(connection)) {
             logger.info(
                    "[{}] Already processing connection/retry for: {}",
                    Thread.currentThread().getName(),
                    blockNodeName(nodeConfig));
            return;
        }

        // Mark for retry *before* scheduling the task
        // Use put, replacing any previous placeholder if necessary.
        connectionsInRetry.put(nodeConfig, connection);
        logger.info(
                "[{}] Marked {} for connection attempt/retry.",
                Thread.currentThread().getName(),
                blockNodeName(nodeConfig));

        long delayMillis = initialDelay.toMillis();
        // Ensure non-negative delay
        if (delayMillis < 0) {
            delayMillis = 0;
        }

        logger.debug(
                "[{}] Scheduling connection task for block node {} in {} ms",
                Thread.currentThread().getName(),
                blockNodeName(nodeConfig),
                delayMillis);

        // Schedule the first attempt using the connectionExecutor
        try {
            // Pass the *actual* initial delay for this sequence, not the fixed INITIAL_RETRY_DELAY
            connectionExecutor.schedule(
                    new RetryTask(connection, initialDelay), delayMillis, TimeUnit.MILLISECONDS);
             logger.info(
                    "[{}] Successfully scheduled connection task for {}.",
                    Thread.currentThread().getName(),
                    blockNodeName(nodeConfig));
        } catch (Exception e) {
            logger.error(
                    "Failed to schedule connection task for block node {}. Removing from retry map.",
                    blockNodeName(nodeConfig),
                    e);
            // If scheduling fails, remove from retry map to revert state
            connectionsInRetry.remove(nodeConfig);
            // Consider closing the connection object if scheduling fails
            connection.close();
        }
    }

    /**
     * Shuts down the connection manager, closing active connection.
     */
    public void shutdown() {
        connectionExecutor.shutdown();
        try {
            if (!connectionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.error("Failed to shut down connection executor within timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            connectionLock.lock();
            try {
                // Ensure activeConnection is not null before closing
                if (activeConnection != null) {
                    activeConnection.close();
                }
                // Also close connections still marked for retry
                connectionsInRetry.values().forEach(conn -> {
                    if (conn != NoOpConnection.INSTANCE) { // Avoid closing placeholder
                        conn.close();
                    }
                });
                connectionsInRetry.clear();
            } finally {
                connectionLock.unlock();
            }
        }
    }

    /**
     * Waits for at least one connection to be established.
     * Initiates connection attempts and waits using a condition variable.
     *
     * @param timeout the maximum time to wait
     * @return true if at least one connection was established, false if the timeout elapsed before any connections were established
     */
    public boolean waitForConnection(Duration timeout) {
        // Initiate connection attempts first by selecting nodes and scheduling them
        selectBlockNodeForStreaming();

        long remainingNanos = timeout.toNanos();

        connectionLock.lock(); // Acquire lock before checking/waiting
        try {
            // Loop while no connection is active
            while (!currentActiveInternal()) { // Use internal check that assumes lock is held
                // Check if timeout expired
                if (remainingNanos <= 0L) {
                    logger.warn("Timeout waiting for initial block node connection after {}", timeout);
                    return false; // Timeout expired
                }

                try {
                    // Wait for a signal or until the remaining timeout elapses
                    logger.debug("No active connection, waiting for signal (timeout: {} ns)", remainingNanos);
                    remainingNanos = connectionEstablishedCondition.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for block node connection", e);
                    return false; // Interrupted
                }
            }
            // If loop finishes, it means currentActiveInternal() is true
            logger.info("Initial block node connection established successfully to {}.", blockNodeName(activeConnection.getNodeConfig()));
            return true;
        } finally {
            connectionLock.unlock(); // Ensure lock is always released
        }
    }

    static String blockNodeName(@Nullable final BlockNodeConfig node) {
        return node != null ? node.address() + ":" + node.port() : "null";
    }

    public void openBlock(long blockNumber) {
        synchronized (connectionLock) {
            // Identify the currently-active connection
            logger.debug(
                    "[{}] openBlock:: Active Connection: {}",
                    Thread.currentThread().getName(),
                    activeConnection);
            final BlockNodeConnection connection = getActiveConnection().orElse(null);
            if (connection == null) {
                logger.warn(
                        "[{}] No active connections available for streaming block {}",
                        Thread.currentThread().getName(),
                        blockNumber);
                return;
            }

            connection.getConnectionStateLock().lock();
            try {
                if (connection.isActive()) {
                    if (connection.getCurrentBlockNumber() == -1) {
                        connection.setCurrentBlockNumber(blockNumber);
                    }
                    connection.notifyNewBlockAvailable();
                }
            } finally {
                connection.getConnectionStateLock().unlock();
            }
        }
    }

    public void notifyConnectionsOfNewRequest() {
        synchronized (connectionLock) {
            final BlockNodeConnection connection = getActiveConnection().orElse(null);
            if (connection == null) {
                return;
            }

            connection.getConnectionStateLock().lock();
            try {
                if (connection.isActive()) {
                    connection.notifyNewRequestAvailable();
                }
            } finally {
                connection.getConnectionStateLock().unlock();
            }
        }
    }

    /**
     * Selects the next highest priority available block node and schedules a connection attempt.
     */
    @VisibleForTesting
    void selectBlockNodeForStreaming() {
        final BlockNodeConfig selectedNode = getNextPriorityBlockNode();
        logger.debug(
                "[{}] Selected block node {} for connection attempt",
                Thread.currentThread().getName(),
                blockNodeName(selectedNode));

        if (selectedNode == null) {
            // This could be the case where all configured block nodes are already being retried
            // Or the initial list was empty.
            // Check if the highest priority from the retried ones is now ready.
            logger.debug(
                    "[{}] No available block nodes found for initial selection/scheduling. Retries might be in progress.",
                    Thread.currentThread().getName());

            // We might already have a connection established via retry before this selection runs again.
            // The waitForConnection loop will handle waiting.
            // If a higher priority node becomes available via retry, updateIfHighest and isHigherPriorityReady handle it.

            // We don't need to check highestPriorityReadyConnection here, as that's primarily for *switching*
            // an existing active connection. waitForConnection handles the initial wait.
             return; // Nothing to schedule initially
        }

        // If we selected a node, schedule the connection attempt.
        connectToNode(selectedNode);
    }

    public BlockNodeConfig getNextPriorityBlockNode() {
        logger.info(
                "[{}] Establishing connection to block node based on priorities",
                Thread.currentThread().getName());
        List<BlockNodeConfig> availableNodes = blockNodeConfigurations.getAllNodes();

        final Map<Integer, List<BlockNodeConfig>> priorityGroups =
                availableNodes.stream().collect(Collectors.groupingBy(BlockNodeConfig::priority));
        final List<Integer> sortedPriorities = new ArrayList<>(priorityGroups.keySet());
        sortedPriorities.sort(Integer::compare);

        // Find the current lowest priority which is the priority of the active connection
        final int currentMinPriority = getCurrentMinPriority();

        BlockNodeConfig selectedNode = null;
        for (Integer priority : sortedPriorities) {
            // Skip over any nodes that have less priority than the current minimum
            // Correction: We want the lowest priority number (highest priority)
            // But we only consider switching *up* in priority, so skip >= currentMinPriority
            if (priority >= currentMinPriority) continue;

            // Filter nodes not in retry, and select one randomly
            final List<BlockNodeConfig> nextPriorityGroup = priorityGroups.get(priority).stream()
                    .filter(node -> !isRetrying(node)) // Check if node is marked for retry
                    .toList();

            if (!nextPriorityGroup.isEmpty()) {
                // Select randomly among available nodes in the highest priority group found so far
                final var randomIndex = random.nextInt(nextPriorityGroup.size());
                selectedNode = nextPriorityGroup.get(randomIndex);
                break; // Found the highest priority group with available nodes
            }
        }
        // If no higher priority node is available, check the current priority group (if active)
        if (selectedNode == null && currentMinPriority != Integer.MAX_VALUE) {
             final List<BlockNodeConfig> currentPriorityGroup = priorityGroups.get(currentMinPriority).stream()
                    .filter(node -> !isRetrying(node)) // Check if node is marked for retry
                    .toList();
             if (!currentPriorityGroup.isEmpty()) {
                 selectedNode = currentPriorityGroup.get(random.nextInt(currentPriorityGroup.size()));
             }
        }

        // If still null, maybe check lower priorities only if NO connection is active?
        // This logic primarily targets finding the BEST node, assuming retries handle eventual connection.
        // If all nodes are in retry, selectedNode remains null, handled by caller.

        return selectedNode;
    }

    private void setCurrentActiveConnection(@NonNull BlockNodeConnection connection) {
        connectionLock.lock(); // Acquire lock before modifying shared state and signaling
        try {
            // Check if we are replacing an existing connection or setting the first one
            BlockNodeConnection oldConnection = this.activeConnection;
            if (oldConnection != connection && !currentActiveInternal()) { // Only set if not already active or setting same one
                this.activeConnection = connection;
                logger.info(
                        "Set active connection to {}. Signaling condition.",
                        blockNodeName(connection.getNodeConfig()));
                // Signal waiters (like waitForConnection) that a connection is now active
                connectionEstablishedCondition.signalAll();
            } else if (oldConnection != connection && currentActiveInternal()) {
                logger.warn(
                        "Attempted to set active connection to {}, but connection {} is already active. Ignoring.",
                        blockNodeName(connection.getNodeConfig()),
                        blockNodeName(oldConnection.getNodeConfig()));
            } else {
                 // Setting the same connection again, or no change needed
                 logger.debug("setCurrentActiveConnection called with the current active connection {} or no change needed.", blockNodeName(connection.getNodeConfig()));
            }
        } finally {
            connectionLock.unlock(); // Ensure lock is always released
        }
    }

    /**
     * Creates a BlockNodeConnection instance and immediately schedules the *first*
     * connection attempt using the retry mechanism (with zero initial delay).
     *
     * @param node The configuration of the node to connect to.
     */
    private void connectToNode(@NonNull BlockNodeConfig node) {
        logger.info("Scheduling connection attempt for block node {}", blockNodeName(node));
        BlockNodeConnection connection = null; // Declare outside try
        try {
            // Create the connection object
            final GrpcServiceClient grpcClient = createNewGrpcClient(node);
            connection = new BlockNodeConnection(
                    node, this, blockStreamStateManager, grpcClient, connectionExecutor, blockStreamMetrics);

            // Immediately schedule the FIRST connection attempt.
            scheduleRetry(connection, Duration.ZERO);

        } catch (Exception e) {
            // Catch potential errors during GrpcServiceClient or BlockNodeConnection creation
            logger.error(
                    "Failed during creation or initial scheduling for block node {}. Node will not be retried until selected again.",
                    blockNodeName(node),
                    e);
            // DO NOT add NoOpConnection to connectionsInRetry here.
            // If creation fails, the node is simply not available now.
            // If scheduleRetry failed, it already logged and cleaned up connectionsInRetry.
        }
    }

    @VisibleForTesting
    Optional<BlockNodeConnection> getActiveConnection() {
        connectionLock.lock(); // Acquire lock before accessing shared state
        try {
            if (currentActiveInternal()) { // Use internal check assuming lock is held
                return Optional.of(activeConnection);
            }
            return Optional.empty();
        } finally {
            connectionLock.unlock(); // Ensure lock is always released
        }
    }

    @VisibleForTesting
    BlockNodeConnection getHighestPriorityReadyConnection() {
        connectionLock.lock(); // Acquire lock before accessing shared state
        try {
            return highestPriorityReadyConnection;
        } finally {
            connectionLock.unlock(); // Ensure lock is always released
        }
    }

    @VisibleForTesting
    boolean isRetrying(BlockNodeConnection connection) {
         if (connection == null) return false;
        return isRetrying(connection.getNodeConfig());
    }

    private boolean isRetrying(BlockNodeConfig config) {
        if (config == null) return false;
        // No external lock needed, ConcurrentHashMap handles its own concurrency for single ops.
        return connectionsInRetry.containsKey(config);
    }

    private int getCurrentMinPriority() {
        connectionLock.lock(); // Acquire lock for consistent read of activeConnection
        try {
            // Use currentActiveInternal as lock is held
            return currentActiveInternal()
                    ? activeConnection.getNodeConfig().priority()
                    : Integer.MAX_VALUE; // If no active connection, return max priority
        } finally {
            connectionLock.unlock();
        }
    }

    // Public method that acquires lock
    public boolean currentActive() {
        connectionLock.lock();
        try {
            return currentActiveInternal();
        } finally {
            connectionLock.unlock();
        }
    }

    // Internal helper that assumes lock is already held by caller
    private boolean currentActiveInternal() {
        return connectionActiveInternal(activeConnection);
    }

    // Internal helper that assumes lock is already held by caller
    private boolean connectionActiveInternal(BlockNodeConnection connection) {
        // Assumes lock is held by caller
        // Check if connection is non-null, not marked for retry, and reports itself as active
        return connection != null && !isRetrying(connection.getNodeConfig()) && connection.isActive();
    }

    private void updateIfHighest(@NonNull final BlockNodeConnection connection) {
        // This method is called from RetryTask.run(), lock must be acquired here.
        connectionLock.lock();
        try {
            if (highestPriorityReadyConnection == null
                    || connection.getNodeConfig().priority()
                            < highestPriorityReadyConnection.getNodeConfig().priority()) {
                highestPriorityReadyConnection = connection;
                logger.debug(
                        "Updated highest priority ready connection to {}",
                        blockNodeName(connection.getNodeConfig()));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    private void cleanupLowerPriorityRetries(int priority) {
        // Although connectionsInRetry is concurrent, we need the lock because the logic
        // is often coupled with reading/writing other locked state (like highestPriorityReadyConnection)
        connectionLock.lock();
        try {
            final List<BlockNodeConfig> toRemove = connectionsInRetry.keySet().stream()
                    .filter(blockNodeConfig -> blockNodeConfig.priority() >= priority)
                    .toList();

            toRemove.forEach(retryConnection -> {
                connectionsInRetry.remove(retryConnection);
                logger.debug("Removed lower priority connection {} from retry state", blockNodeName(retryConnection));
            });
        } finally {
            connectionLock.unlock();
        }
    }

    /**
     * @param activeConnectionParam if there is a higher priority connection ready to be established
     * @return whether there is a higher priority connection ready
     */
    public boolean isHigherPriorityReady(@NonNull final BlockNodeConnection activeConnectionParam) {
        connectionLock.lock(); // Acquire lock for consistent state access/modification
        try {
            // Re-verify the passed connection is still the active one under lock
            if (this.activeConnection != activeConnectionParam || !currentActiveInternal()) {
                logger.warn("isHigherPriorityReady called with a connection that is no longer active.");
                return false;
            }

            if (highestPriorityReadyConnection == null) {
                return false; // No higher priority connection ready
            }

            final BlockNodeConnection highestAvailableConnection = highestPriorityReadyConnection;
            final int highestAvailablePriority =
                    highestAvailableConnection.getNodeConfig().priority();
            final int currentActivePriority = activeConnectionParam.getNodeConfig().priority();

            if (highestAvailablePriority >= currentActivePriority) {
                return false; // Highest ready is not strictly higher priority
            }

            logger.info(
                    "Higher priority connection {} ready. Switching from {}.",
                    blockNodeName(highestAvailableConnection.getNodeConfig()),
                    blockNodeName(activeConnectionParam.getNodeConfig()));

            // Clear the highest ready marker *before* closing old/setting new
            this.highestPriorityReadyConnection = null;

            // Cleanup retries for nodes with priority >= new highest (which is taking over)
            cleanupLowerPriorityRetries(highestAvailablePriority); // Handles locking internally

            final long blockToStartTheNewConnection;
            long currentBlockNum = activeConnectionParam.getCurrentBlockNumber();
            blockToStartTheNewConnection = (currentBlockNum == -1L) ? -1L : currentBlockNum + 1L;

            // Close current active connection
            activeConnectionParam.close();
            // Explicitly null out activeConnection before setting the new one via setCurrentActiveConnection
            // to ensure currentActiveInternal() returns false inside setCurrentActiveConnection.
            this.activeConnection = null;

            // Set the higher priority connection as the new current connection (internally signals condition)
            setCurrentActiveConnection(highestAvailableConnection); // Handles locking & signaling

            // Activate the new connection and tell it where to start (if applicable)
            // Need to ensure activeConnection is set by now, which setCurrentActiveConnection should do.
            if (this.activeConnection == highestAvailableConnection) {
                 highestAvailableConnection.updateConnectionState(BlockNodeConnection.ConnectionState.ACTIVE);
                 if(blockToStartTheNewConnection != -1L) {
                     highestAvailableConnection.jumpToBlock(blockToStartTheNewConnection);
                 }
                 logger.info("Successfully switched to higher priority connection {}.", blockNodeName(highestAvailableConnection.getNodeConfig()));
                 return true;
            } else {
                // This case should ideally not happen if locking is correct
                logger.error("Failed to switch active connection during priority handover. Expected {} but found {}.",
                     blockNodeName(highestAvailableConnection.getNodeConfig()),
                     this.activeConnection != null ? blockNodeName(this.activeConnection.getNodeConfig()) : "null");
                // Attempt to recover? Maybe schedule retry for highestAvailableConnection?
                 if (highestAvailableConnection != null) {
                     scheduleRetry(highestAvailableConnection, INITIAL_RETRY_DELAY);
                 }
                return false;
            }
        } finally {
            connectionLock.unlock(); // Ensure lock is always released
        }
    }

    // This class exists solely to avoid checking for null every time we reference a connection in connectionsInRetry
    // And to handle cases where connection object creation might fail in connectToNode
    private static class NoOpConnection extends BlockNodeConnection {
        static final NoOpConnection INSTANCE;

        static {
            INSTANCE = new NoOpConnection();
        }

        private NoOpConnection() {
            // Provide minimal valid state for super constructor if needed, or make super allow nulls
            // Assuming BlockNodeConfig.DEFAULT exists and manager can be null for this placeholder
            super(BlockNodeConfig.DEFAULT, null, null, null, null, null);
        }

        @Override
        public void establishStream() {
            // No-op
        }

        @Override
        public void onNext(PublishStreamResponse response) {
            // No-op
        }

        @Override
        public void onError(Throwable throwable) {
            // No-op
        }

        @Override
        public void onCompleted() {
            // No-op
        }

        @Override
        public void sendRequest(@NonNull final PublishStreamRequest request) {
            // No-op
        }

        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public void close() {
            // No-op
        }

        @Override
        public BlockNodeConfig getNodeConfig() {
            return BlockNodeConfig.DEFAULT; // Return default or null?
        }

        @Override
        public ReentrantLock getConnectionStateLock() {
             // Return a dummy lock or handle null checks where used
             return new ReentrantLock();
        }

        @Override
        public void updateConnectionState(ConnectionState newState) { /* No-op */ }

        @Override
        public void setCurrentBlockNumber(long blockNumber) { /* No-op */ }

        @Override
        public long getCurrentBlockNumber() { return -1L; }

        @Override
        public void notifyNewBlockAvailable() { /* No-op */ }

        @Override
        public void notifyNewRequestAvailable() { /* No-op */ }

        @Override
        public void jumpToBlock(long blockNumber) { /* No-op */ }
    }

    /**
     * @param blockNodeConfig the configuration for the block node
     * @param blockNumber the block number of the last verified block
     */
    public void updateLastVerifiedBlock(
            @NonNull final BlockNodeConfig blockNodeConfig, @Nullable final Long blockNumber) {
        requireNonNull(blockNodeConfig);

        final Long latestBlock = getLastVerifiedBlock(blockNodeConfig);
        if (blockNumber != null && blockNumber > latestBlock) {
            lastVerifiedBlockPerConnection.put(blockNodeConfig, blockNumber);
        } else {
            logger.warn(
                    "Attempted to update connection {} with invalid block number {} (highest {})",
                    blockNodeConfig,
                    blockNumber,
                    latestBlock);
        }
    }

    /**
     * @param blockNodeConfig the configuration for the block node
     * @return the last verified block number by the given block node.
     */
    public Long getLastVerifiedBlock(@NonNull final BlockNodeConfig blockNodeConfig) {
        requireNonNull(blockNodeConfig);
        return lastVerifiedBlockPerConnection.computeIfAbsent(blockNodeConfig, key -> -1L);
    }

    /**
     * @param blockNumber the block number to check for acknowledgements
     * @return whether the block has been acknowledged by any connection.
     */
    public boolean isBlockAlreadyAcknowledged(@NonNull final Long blockNumber) {
        requireNonNull(blockNumber);
        return lastVerifiedBlockPerConnection.values().stream()
                .anyMatch(verifiedBlock -> verifiedBlock.equals(blockNumber));
    }

    /**
     * Runnable task to handle the connection attempt logic.
     * Schedules itself for subsequent retries upon failure using the connectionExecutor.
     * Handles setting active connection and signaling on success.
     */
    private class RetryTask implements Runnable {
        private final BlockNodeConnection connection;
        private Duration currentBackoffDelay; // Represents the delay *before* the next attempt

        RetryTask(@NonNull final BlockNodeConnection connection, @NonNull final Duration initialDelay) {
            this.connection = requireNonNull(connection);
            // Ensure initial delay is non-negative for backoff calculation
            this.currentBackoffDelay = initialDelay.isNegative() ? Duration.ZERO : initialDelay;
        }

        @Override
        public void run() {
             // Log immediately upon entry to confirm execution start
             logger.info("[{}] Entered RetryTask.run() for {}", Thread.currentThread().getName(), blockNodeName(connection.getNodeConfig()));

             final var nodeConfig = connection.getNodeConfig();
             try {
                 logger.debug("[{}] Entering try block in RetryTask for {}", Thread.currentThread().getName(), blockNodeName(nodeConfig));
                 logger.info(
                         "[{}] Attempting to establish stream to block node {}",
                         Thread.currentThread().getName(),
                         blockNodeName(nodeConfig));
                 logger.debug("[{}] Calling updateConnectionState(RETRYING) in RetryTask for {}", Thread.currentThread().getName(), blockNodeName(nodeConfig));
                 // Ensure state reflects attempt is happening (might already be RETRYING if scheduled via error)
                 connection.updateConnectionState(BlockNodeConnection.ConnectionState.RETRYING);
                 logger.debug("[{}] Returned from updateConnectionState(RETRYING) in RetryTask for {}", Thread.currentThread().getName(), blockNodeName(nodeConfig));

                 // Attempt to establish the stream
                 connection.establishStream();

                 // --- Success Path ---
                 logger.info(
                         "[{}] Successfully established stream to block node {}",
                         Thread.currentThread().getName(),
                         blockNodeName(nodeConfig));
                 // Remove from retry map *before* setting active connection
                 connectionsInRetry.remove(nodeConfig);
                 // Set as active (if appropriate) and signal condition
                 setCurrentActiveConnection(connection);
                 // Update highest priority connection if needed (acquires lock internally)
                 updateIfHighest(connection);

             } catch (Exception e) {
                 // --- Failure Path ---
                 logger.warn(
                         "[{}] Stream establishment attempt failed for block node {}: {}. Rescheduling.",
                         Thread.currentThread().getName(),
                         blockNodeName(nodeConfig),
                         e.getMessage()); // Log only message on expected retry failures

                 // Calculate next delay based on the *previous* backoff delay for this task instance
                 Duration nextDelay = currentBackoffDelay.isZero()
                         ? INITIAL_RETRY_DELAY // Start with initial delay if previous was 0
                         : currentBackoffDelay.multipliedBy(RETRY_BACKOFF_MULTIPLIER);

                 if (nextDelay.compareTo(MAX_RETRY_DELAY) > 0) {
                     nextDelay = MAX_RETRY_DELAY;
                 }

                 // Apply jitter
                 long jitteredDelayMs;
                  if (nextDelay.toMillis() > 0) {
                     jitteredDelayMs = nextDelay.toMillis() / 2 + random.nextLong(nextDelay.toMillis() / 2 + 1);
                  } else {
                      // Should not happen if INITIAL_RETRY_DELAY > 0, but handle defensively
                     jitteredDelayMs = INITIAL_RETRY_DELAY.toMillis() / 2 + random.nextLong(INITIAL_RETRY_DELAY.toMillis() / 2 + 1);
                      if (jitteredDelayMs <= 0) jitteredDelayMs = 1; // Ensure positive delay
                  }

                 // Update backoff delay *for the next run* of this task instance
                 this.currentBackoffDelay = Duration.ofMillis(jitteredDelayMs);

                 // Reschedule this task using the calculated jittered delay
                 try {
                     logger.debug(
                             "[{}] Scheduling next connection attempt for {} in {} ms",
                             Thread.currentThread().getName(),
                             blockNodeName(nodeConfig),
                             jitteredDelayMs);
                     connectionExecutor.schedule(this, jitteredDelayMs, TimeUnit.MILLISECONDS);
                 } catch (Exception scheduleEx) {
                     logger.error(
                             "Failed to reschedule connection attempt for block node {}. Removing from retry map.",
                             blockNodeName(nodeConfig),
                             scheduleEx);
                     // If rescheduling fails, remove from retry map to prevent inconsistent state
                     connectionsInRetry.remove(nodeConfig);
                     // We might also want to explicitly close the connection object here if rescheduling fails catastrophically
                     connection.close();
                 }
             }
        }
    }
}
