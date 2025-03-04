// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.spec.utilops;

import com.hedera.hapi.block.protoc.PublishStreamResponseCode;

/**
 * Utility verbs for interacting with block node simulators.
 */
public class BlockNodeSimulatorVerbs {
    private BlockNodeSimulatorVerbs() {
        // Utility class
    }

    /**
     * Creates a builder for interacting with block node simulators.
     * This is a convenience method that uses a more fluent naming convention.
     *
     * @return a builder for the operation
     */
    public static BlockNodeSimulatorBuilder blockNodeSimulator() {
        return new BlockNodeSimulatorBuilder();
    }

    /**
     * Builder for block node simulator operations.
     */
    public static class BlockNodeSimulatorBuilder {
        /**
         * Creates a builder for sending an immediate EndOfStream response to a block node simulator.
         *
         * @param nodeIndex the index of the block node simulator (0-based)
         * @param responseCode the response code to send
         * @return a builder for the operation
         */
        public BlockNodeSimulatorOp.SendEndOfStreamBuilder sendEndOfStreamImmediately(
                int nodeIndex, PublishStreamResponseCode responseCode) {
            return BlockNodeSimulatorOp.sendEndOfStreamImmediately(nodeIndex, responseCode);
        }

        /**
         * Creates a builder for shutting down a specific block node simulator immediately.
         *
         * @param nodeIndex the index of the block node simulator (0-based)
         * @return a builder for the operation
         */
        public BlockNodeSimulatorOp.ShutdownBuilder shutDownImmediately(int nodeIndex) {
            return BlockNodeSimulatorOp.shutdownImmediately(nodeIndex);
        }

        /**
         * Creates a builder for shutting down all block node simulators immediately.
         *
         * @return a builder for the operation
         */
        public BlockNodeSimulatorOp.ShutdownAllBuilder shutDownAll() {
            return BlockNodeSimulatorOp.shutdownAll();
        }

        /**
         * Creates a builder for restarting a specific block node simulator immediately.
         *
         * @param nodeIndex the index of the block node simulator (0-based)
         * @return a builder for the operation
         */
        public BlockNodeSimulatorOp.RestartBuilder restartImmediately(int nodeIndex) {
            return BlockNodeSimulatorOp.restartImmediately(nodeIndex);
        }

        /**
         * Creates a builder for restarting all previously shutdown block node simulators.
         *
         * @return a builder for the operation
         */
        public BlockNodeSimulatorOp.RestartAllBuilder restartAll() {
            return BlockNodeSimulatorOp.restartAll();
        }

        /**
         * Creates a builder for asserting that a specific block has been received by a block node simulator.
         *
         * @param nodeIndex the index of the block node simulator (0-based)
         * @param blockNumber the block number to check
         * @return a builder for the operation
         */
        public BlockNodeSimulatorOp.AssertBlockReceivedBuilder assertBlockReceived(int nodeIndex, long blockNumber) {
            return BlockNodeSimulatorOp.assertBlockReceived(nodeIndex, blockNumber);
        }

        /**
         * Creates a builder for getting the last verified block number from a block node simulator.
         *
         * @param nodeIndex the index of the block node simulator (0-based)
         * @return a builder for the operation
         */
        public BlockNodeSimulatorOp.GetLastVerifiedBlockBuilder getLastVerifiedBlock(int nodeIndex) {
            return BlockNodeSimulatorOp.getLastVerifiedBlock(nodeIndex);
        }
    }
}
