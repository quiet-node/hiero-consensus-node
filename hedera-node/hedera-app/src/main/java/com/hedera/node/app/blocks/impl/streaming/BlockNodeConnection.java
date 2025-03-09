// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.node.internal.network.BlockNodeConfig;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.helidon.webclient.grpc.GrpcServiceClient;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a single connection to a block node. Each connection is responsible for connecting to configured block nodes
 */
public class BlockNodeConnection {
    private static final Logger logger = LogManager.getLogger(BlockNodeConnection.class);

    private final BlockNodeConfig node;
    private final GrpcServiceClient grpcServiceClient;
    private final BlockNodeConnectionManager manager;
    private final BlockStreamStateManager blockStreamStateManager;
    private StreamObserver<PublishStreamRequest> requestObserver;

    private final ReentrantLock isActiveLock = new ReentrantLock();
    private volatile boolean isActive = true;
    
    // Variables for tracking the current block and request index
    private long currentBlockNumber = -1;
    private int currentRequestIndex = 0;
    
    // Worker thread for submitting requests
    private Thread requestWorker;

    public BlockNodeConnection(
            BlockNodeConfig nodeConfig, 
            GrpcServiceClient grpcServiceClient, 
            BlockNodeConnectionManager manager,
            BlockStreamStateManager blockStreamStateManager) {
        this.node = nodeConfig;
        this.grpcServiceClient = grpcServiceClient;
        this.manager = manager;
        this.blockStreamStateManager = blockStreamStateManager;
        establishStream();
        startRequestWorker();
        logger.info("BlockNodeConnection INITIALIZED");
    }

    private void establishStream() {
        requestObserver =
                grpcServiceClient.bidi(manager.getGrpcEndPoint(), new StreamObserver<PublishStreamResponse>() {
                    @Override
                    public void onNext(PublishStreamResponse response) {
                        if (response.hasAcknowledgement()) {
                            handleAcknowledgement(response.getAcknowledgement());
                        } else if (response.hasEndStream()) {
                            handleEndOfStream(response.getEndStream());
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
        isActiveLock.lock();
        isActive = true;
        isActiveLock.unlock();
    }
    
    private void startRequestWorker() {
        requestWorker = new Thread(this::requestWorkerLoop);
        requestWorker.setName("BlockNodeConnection-RequestWorker-" + node.address() + ":" + node.port());
        requestWorker.start();
        logger.info("Started request worker thread for block node {}:{}", node.address(), node.port());
    }
    
    private void requestWorkerLoop() {
        while (isActive) {
            try {
                // Wait for a block to be available if we don't have one yet
                while (isActive && (currentBlockNumber == -1||
                        blockStreamStateManager.getBlockState(currentBlockNumber) == null ||
                        blockStreamStateManager.getBlockState(currentBlockNumber).isComplete())) {
                    logger.info("Waiting for new block to be available for node {}:{}", node.address(), node.port());
                    blockStreamStateManager.getNewBlockAvailableCondition().await();
                }

                // Check if we should exit the loop
                if (!isActive) {
                    break;
                }

                // Get the current block state
                BlockState blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
                if (blockState == null) {
                    // This shouldn't happen due to the condition above, but just in case
                    logger.warn("Block state is null for block {} on node {}:{}",
                            currentBlockNumber, node.address(), node.port());
                    continue;
                }

                // Wait if we've caught up to the available requests
                while (isActive && currentRequestIndex > blockState.requests().size() && !blockState.isComplete()) {
                    logger.info("Waiting for new requests to be available for block {} on node {}:{}",
                            currentBlockNumber, node.address(), node.port());
                    blockStreamStateManager.getNewRequestAvailableCondition().await();
                }

                // Check if we should exit the loop
                if (!isActive) {
                    break;
                }

                // Process available requests
                while (isActive && currentRequestIndex < blockState.requests().size()) {
                    PublishStreamRequest request = blockState.requests().get(currentRequestIndex);
                    sendRequest(request);
                    currentRequestIndex++;
                }

                // Check if we should exit the loop
                if (!isActive) {
                    break;
                }

                // If the block is complete and we've sent all requests, move to the next block
                if (blockState.isComplete() && currentRequestIndex >= blockState.requests().size()) {
                    logger.info("Completed sending all requests for block {} to node {}:{}",
                            currentBlockNumber, node.address(), node.port());
                    currentBlockNumber++;
                    currentRequestIndex = 0;
                }
            } catch (InterruptedException e) {
                if (isActive) {
                    logger.error("Request worker thread interrupted for node {}:{}", node.address(), node.port(), e);
                }
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in request worker thread for node {}:{}", node.address(), node.port(), e);
            }
        }
        logger.info("Request worker thread exiting for node {}:{}", node.address(), node.port());
    }

    private void handleAcknowledgement(PublishStreamResponse.Acknowledgement acknowledgement) {
        if (acknowledgement.hasBlockAck()) {
            logger.info("Block acknowledgment received for a full block: {}", acknowledgement.getBlockAck());
        }
    }

    private void handleEndOfStream(PublishStreamResponse.EndOfStream endOfStream) {
        logger.info("Error returned from block node at block number {}: {}", endOfStream.getBlockNumber(), endOfStream);
    }

    private void removeFromActiveConnections(BlockNodeConfig node) {
        manager.handleConnectionError(node);
    }

    public void handleStreamFailure() {
        isActiveLock.lock();
        isActive = false;
        isActiveLock.unlock();
        blockStreamStateManager.getLock().lock();
        try {
            // Signal any waiting threads to check the isActive flag
            blockStreamStateManager.getNewBlockAvailableCondition().signalAll();
            blockStreamStateManager.getNewRequestAvailableCondition().signalAll();
        } finally {
            blockStreamStateManager.getLock().unlock();
        }
        removeFromActiveConnections(node);
    }

    public void sendRequest(PublishStreamRequest request) {
        if (isActive) {
            requestObserver.onNext(request);
        }
    }

    public void close() {
        if (isActive) {
            isActiveLock.lock();
            isActive = false;
            isActiveLock.unlock();
            try {
                blockStreamStateManager.getLock().lock();
                // Signal any waiting threads to check the isActive flag
                blockStreamStateManager.getNewBlockAvailableCondition().signalAll();
                blockStreamStateManager.getNewRequestAvailableCondition().signalAll();
            } finally {
                blockStreamStateManager.getLock().unlock();
            }
            
            // Wait for the worker thread to terminate
            try {
                if (requestWorker != null) {
                    requestWorker.join(5000); // Wait up to 5 seconds
                    if (requestWorker.isAlive()) {
                        logger.warn("Request worker thread did not terminate within timeout for node {}:{}", 
                                node.address(), node.port());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for request worker thread to terminate", e);
            }
            
            requestObserver.onCompleted();
        }
    }

    public boolean isActive() {
        return isActive;
    }

    public BlockNodeConfig getNodeConfig() {
        return node;
    }
    
    /**
     * Gets the current block number being processed.
     * 
     * @return the current block number, or -1 if no block is being processed
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
}
