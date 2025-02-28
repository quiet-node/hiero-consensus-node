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
     * Creates a builder for sending an immediate EndOfStream response to a block node simulator.
     *
     * @param nodeIndex the index of the block node simulator (0-based)
     * @param responseCode the response code to send
     * @return a builder for the operation
     */
    public static BlockNodeSimulatorOp.SendEndOfStreamBuilder blockNodeSimulator_sendEndOfStreamImmediately(
            int nodeIndex, PublishStreamResponseCode responseCode) {
        return BlockNodeSimulatorOp.sendEndOfStreamImmediately(nodeIndex, responseCode);
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
    }
}
