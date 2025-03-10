// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.node.internal.network.BlockNodeConfig;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.helidon.webclient.grpc.GrpcServiceClient;
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
    private final Object newBlockAvailable = new Object();
    private final Object newRequestAvailable = new Object();
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
        logger.info("Establishing stream to block node {}:{}", node.address(), node.port());
        try {
            requestObserver =
                    grpcServiceClient.bidi(manager.getGrpcEndPoint(), new StreamObserver<PublishStreamResponse>() {
                        @Override
                        public void onNext(PublishStreamResponse response) {
                            logger.info("Received response from block node {}:{}", node.address(), node.port());
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
            logger.info("Successfully established stream to block node {}:{}", node.address(), node.port());
        } catch (Exception e) {
            logger.error("Failed to establish stream to block node {}:{}", node.address(), node.port(), e);
            handleStreamFailure();
        }
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
                if (currentBlockNumber == -1 || 
                        blockStreamStateManager.getBlockState(currentBlockNumber) == null) {
                    logger.info("Waiting for new block to be available for node {}:{}", node.address(), node.port());
                    synchronized (newBlockAvailable) {
                        newBlockAvailable.wait();
                    }
                    continue;
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

                logger.info("Processing block {} for node {}:{}, isComplete: {}, requests: {}",
                        currentBlockNumber, node.address(), node.port(), 
                        blockState.isComplete(), blockState.requests().size());
                
                // Process available requests
                int requestsSize = blockState.requests().size();
                // If there are no requests yet, wait for some to be added
                if (requestsSize == 0 && !blockState.isComplete()) {
                    logger.info("No requests available for block {} on node {}:{}, waiting...",
                            currentBlockNumber, node.address(), node.port());
                    synchronized (newRequestAvailable) {
                        newRequestAvailable.wait();
                    }
                    // Refresh the block state and requests size after waiting
                    blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
                    if (blockState == null) {
                        continue;
                    }
                    requestsSize = blockState.requests().size();
                }
                
                // If we've processed all available requests but the block isn't complete,
                // wait for more requests to be added
                if (currentRequestIndex >= requestsSize && !blockState.isComplete()) {
                    logger.info("Waiting for new requests to be available for block {} on node {}:{}, " +
                            "currentRequestIndex: {}, requestsSize: {}",
                            currentBlockNumber, node.address(), node.port(), 
                            currentRequestIndex, requestsSize);
                    synchronized (newRequestAvailable) {
                        newRequestAvailable.wait();
                    }
                    // Refresh the block state and requests size after waiting
                    blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
                    if (blockState == null) {
                        continue;
                    }
                    requestsSize = blockState.requests().size();
                    logger.info("After wait - Block: {}, currentRequestIndex: {}, requestsSize: {}, isComplete: {}", 
                            currentBlockNumber, currentRequestIndex, requestsSize, blockState.isComplete());
                }
                
                // Process any available requests
                while (currentRequestIndex < requestsSize) {
                    PublishStreamRequest request = blockState.requests().get(currentRequestIndex);
                    logger.info("Sending request for block {} request index {} to node {}:{}, items: {}",
                            currentBlockNumber, currentRequestIndex, node.address(), node.port(),
                            request.getBlockItems().getBlockItemsCount());
                    sendRequest(request);
                    currentRequestIndex++;
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
                // If we get an IndexOutOfBoundsException, reset the request index to a safe value
                if (e instanceof IndexOutOfBoundsException) {
                    BlockState blockState = blockStreamStateManager.getBlockState(currentBlockNumber);
                    if (blockState != null) {
                        int safeIndex = Math.min(blockState.requests().size(), currentRequestIndex);
                        logger.info("Resetting request index from {} to {} for block {}", 
                                currentRequestIndex, safeIndex, currentBlockNumber);
                        currentRequestIndex = safeIndex;
                    }
                }
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
        removeFromActiveConnections(node);
    }

    public void sendRequest(PublishStreamRequest request) {
        if (isActive) {
            try {
                logger.info("Sending request to node {}:{}: {}", node.address(), node.port(), 
                        request.getBlockItems().getBlockItemsCount());
                requestObserver.onNext(request);
                logger.info("Successfully sent request to node {}:{}", node.address(), node.port());
            } catch (Exception e) {
                logger.error("Failed to send request to node {}:{}", node.address(), node.port(), e);
                handleStreamFailure();
            }
        } else {
            logger.warn("Attempted to send request to inactive connection for node {}:{}", 
                    node.address(), node.port());
        }
    }

    public void close() {
        if (isActive) {
            isActiveLock.lock();
            isActive = false;
            isActiveLock.unlock();
            
            try {
                if (requestWorker != null) {
                    requestWorker.interrupt();
                    requestWorker.join(1000);
                }
            } catch (InterruptedException e) {
                logger.error("Error joining request worker thread for node {}:{}", node.address(), node.port(), e);
                Thread.currentThread().interrupt();
            }
            logger.info("Closed connection to block node {}:{}", node.address(), node.port());
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
                logger.info("Notifying of new request available for node {}:{} - block: {}, requests: {}, isComplete: {}", 
                        node.address(), node.port(), currentBlockNumber, 
                        blockState.requests().size(), blockState.isComplete());
            } else {
                logger.info("Notifying of new request available for node {}:{} - block: {} (state not found)", 
                        node.address(), node.port(), currentBlockNumber);
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
        logger.info("Set current block number to {} for node {}:{}, reset request index to 0", 
                blockNumber, node.address(), node.port());
    }
}
