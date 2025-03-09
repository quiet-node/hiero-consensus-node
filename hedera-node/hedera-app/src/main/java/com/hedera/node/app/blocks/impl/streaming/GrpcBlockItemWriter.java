// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static java.util.Objects.requireNonNull;

import com.hedera.node.app.blocks.BlockItemWriter;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Implements the bidirectional streaming RPC for the publishBlockStream rpc in BlockStreamService.
 */
public class GrpcBlockItemWriter implements BlockItemWriter {
    private static final Logger logger = LogManager.getLogger(GrpcBlockItemWriter.class);
    private final BlockNodeConnectionManager connectionManager;
    private final BlockStreamStateManager blockStreamStateManager;

    /**
     * Construct a new GrpcBlockItemWriter.
     *
     * @param connectionManager the connection manager for the gRPC block stream service
     * @param blockStreamStateManager the block stream state manager
     */
    public GrpcBlockItemWriter(
            @NonNull final BlockNodeConnectionManager connectionManager,
            @NonNull final BlockStreamStateManager blockStreamStateManager) {
        this.connectionManager = requireNonNull(connectionManager, "connectionManager must not be null");
        this.blockStreamStateManager = requireNonNull(blockStreamStateManager, "blockStreamStateManager must not be null");
    }

    @Override
    public void openBlock(long blockNumber) {
        if (blockNumber < 0) throw new IllegalArgumentException("Block number must be non-negative");

        blockStreamStateManager.openBlock(blockNumber);
        logger.info("Started new block in GrpcBlockItemWriter {}", blockNumber);
    }

    @Override
    public void writePbjItem(@NonNull Bytes bytes) {
        requireNonNull(bytes, "bytes must not be null");
        blockStreamStateManager.addItem(bytes);
    }

    @Override
    public void writeItem(@NonNull byte[] bytes) {
        throw new UnsupportedOperationException("writeItem is not supported in this implementation");
    }

    @Override
    public void closeBlock() {
        blockStreamStateManager.closeBlock();
        logger.info("Closed block in GrpcBlockItemWriter");
    }
}
