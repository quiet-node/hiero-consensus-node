// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.spec.utilops;

import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
import com.hedera.services.bdd.junit.hedera.simulator.BlockNodeSimulatorController;
import com.hedera.services.bdd.junit.hedera.subprocess.SubProcessNetwork;
import com.hedera.services.bdd.spec.HapiSpec;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility operation for interacting with the block node simulator.
 */
public class BlockNodeSimulatorOp extends UtilOp {
    private static final Logger log = LogManager.getLogger(BlockNodeSimulatorOp.class);

    private final int nodeIndex;
    private final BlockNodeSimulatorAction action;
    private final PublishStreamResponseCode responseCode;
    private final long blockNumber;
    private final AtomicLong lastVerifiedBlockNumber;
    private final Consumer<Long> lastVerifiedBlockConsumer;

    private BlockNodeSimulatorOp(
            int nodeIndex,
            BlockNodeSimulatorAction action,
            PublishStreamResponseCode responseCode,
            long blockNumber,
            AtomicLong lastVerifiedBlockNumber,
            Consumer<Long> lastVerifiedBlockConsumer) {
        this.nodeIndex = nodeIndex;
        this.action = action;
        this.responseCode = responseCode;
        this.blockNumber = blockNumber;
        this.lastVerifiedBlockNumber = lastVerifiedBlockNumber;
        this.lastVerifiedBlockConsumer = lastVerifiedBlockConsumer;
    }

    @Override
    protected boolean submitOp(HapiSpec spec) throws Throwable {
        if (!(spec.targetNetworkOrThrow() instanceof SubProcessNetwork network)) {
            throw new IllegalStateException("Block node simulator operations require a SubProcessNetwork");
        }

        BlockNodeSimulatorController controller = network.getBlockNodeSimulatorController();
        long verifiedBlock = 0;

        switch (action) {
            case SEND_END_OF_STREAM_IMMEDIATELY:
                verifiedBlock = controller.sendEndOfStreamImmediately(nodeIndex, responseCode, blockNumber);
                log.info(
                        "Sent immediate EndOfStream response with code {} for block {} on simulator {}, last verified block: {}",
                        responseCode,
                        blockNumber,
                        nodeIndex,
                        verifiedBlock);
                break;
            case SET_END_OF_STREAM_RESPONSE:
                controller.setEndOfStreamResponse(nodeIndex, responseCode, blockNumber);
                verifiedBlock = controller.getLastVerifiedBlockNumber(nodeIndex);
                log.info(
                        "Set EndOfStream response code {} for block {} on simulator {}, last verified block: {}",
                        responseCode,
                        blockNumber,
                        nodeIndex,
                        verifiedBlock);
                break;
            case RESET_RESPONSES:
                controller.resetResponses(nodeIndex);
                verifiedBlock = controller.getLastVerifiedBlockNumber(nodeIndex);
                log.info("Reset all responses on simulator {} to default behavior", nodeIndex);
                break;
            case SHUTDOWN_SIMULATOR:
                controller.shutdownSimulator(nodeIndex);
                log.info("Shutdown simulator {} to simulate connection drop", nodeIndex);
                break;
            case RESTART_SIMULATOR:
                if (!controller.isSimulatorShutdown(nodeIndex)) {
                    log.error("Cannot restart simulator {} because it has not been shut down", nodeIndex);
                    return false;
                }
                try {
                    controller.restartSimulator(nodeIndex);
                    log.info("Restarted simulator {}", nodeIndex);
                } catch (IOException e) {
                    log.error("Failed to restart simulator {}", nodeIndex, e);
                    return false;
                }
                break;
            case SHUTDOWN_ALL_SIMULATORS:
                controller.shutdownAllSimulators();
                log.info("Shutdown all simulators to simulate connection drops");
                break;
            case RESTART_ALL_SIMULATORS:
                if (!controller.areAnySimulatorsShutdown()) {
                    log.error("Cannot restart simulators because none have been shut down");
                    return false;
                }
                try {
                    controller.restartAllSimulators();
                    log.info("Restarted all previously shutdown simulators");
                } catch (IOException e) {
                    log.error("Failed to restart simulators", e);
                    return false;
                }
                break;
        }

        if (lastVerifiedBlockNumber != null) {
            lastVerifiedBlockNumber.set(verifiedBlock);
        }

        if (lastVerifiedBlockConsumer != null) {
            lastVerifiedBlockConsumer.accept(verifiedBlock);
        }

        return true;
    }

    /**
     * Enum defining the possible actions to perform on a block node simulator.
     */
    public enum BlockNodeSimulatorAction {
        SEND_END_OF_STREAM_IMMEDIATELY,
        SET_END_OF_STREAM_RESPONSE,
        RESET_RESPONSES,
        SHUTDOWN_SIMULATOR,
        RESTART_SIMULATOR,
        SHUTDOWN_ALL_SIMULATORS,
        RESTART_ALL_SIMULATORS
    }

    /**
     * Creates a builder for sending an immediate EndOfStream response to a block node simulator.
     *
     * @param nodeIndex the index of the block node simulator (0-based)
     * @param responseCode the response code to send
     * @return a builder for the operation
     */
    public static SendEndOfStreamBuilder sendEndOfStreamImmediately(
            int nodeIndex, PublishStreamResponseCode responseCode) {
        return new SendEndOfStreamBuilder(nodeIndex, responseCode);
    }

    /**
     * Creates a builder for shutting down a specific block node simulator immediately.
     *
     * @param nodeIndex the index of the block node simulator (0-based)
     * @return a builder for the operation
     */
    public static ShutdownBuilder shutdownImmediately(int nodeIndex) {
        return new ShutdownBuilder(nodeIndex);
    }

    /**
     * Creates a builder for shutting down all block node simulators immediately.
     *
     * @return a builder for the operation
     */
    public static ShutdownAllBuilder shutdownAll() {
        return new ShutdownAllBuilder();
    }

    /**
     * Creates a builder for restarting a specific block node simulator immediately.
     *
     * @param nodeIndex the index of the block node simulator (0-based)
     * @return a builder for the operation
     */
    public static RestartBuilder restartImmediately(int nodeIndex) {
        return new RestartBuilder(nodeIndex);
    }

    /**
     * Creates a builder for restarting all previously shutdown block node simulators.
     *
     * @return a builder for the operation
     */
    public static RestartAllBuilder restartAll() {
        return new RestartAllBuilder();
    }

    /**
     * Builder for sending an immediate EndOfStream response to a block node simulator.
     */
    public static class SendEndOfStreamBuilder {
        private final int nodeIndex;
        private final PublishStreamResponseCode responseCode;
        private long blockNumber = 0;
        private AtomicLong lastVerifiedBlockNumber;
        private Consumer<Long> lastVerifiedBlockConsumer;

        private SendEndOfStreamBuilder(int nodeIndex, PublishStreamResponseCode responseCode) {
            this.nodeIndex = nodeIndex;
            this.responseCode = responseCode;
        }

        /**
         * Sets the block number to include in the EndOfStream response.
         *
         * @param blockNumber the block number
         * @return this builder
         */
        public SendEndOfStreamBuilder withBlockNumber(long blockNumber) {
            this.blockNumber = blockNumber;
            return this;
        }

        /**
         * Sets an AtomicLong to store the last verified block number.
         *
         * @param lastVerifiedBlockNumber the AtomicLong to store the last verified block number
         * @return this builder
         */
        public SendEndOfStreamBuilder exposingLastVerifiedBlockNumber(AtomicLong lastVerifiedBlockNumber) {
            this.lastVerifiedBlockNumber = lastVerifiedBlockNumber;
            return this;
        }

        /**
         * Sets a consumer to receive the last verified block number.
         *
         * @param lastVerifiedBlockConsumer the consumer to receive the last verified block number
         * @return this builder
         */
        public SendEndOfStreamBuilder exposingLastVerifiedBlockNumber(Consumer<Long> lastVerifiedBlockConsumer) {
            this.lastVerifiedBlockConsumer = lastVerifiedBlockConsumer;
            return this;
        }

        /**
         * Builds the operation.
         *
         * @return the operation
         */
        public BlockNodeSimulatorOp build() {
            return new BlockNodeSimulatorOp(
                    nodeIndex,
                    BlockNodeSimulatorAction.SEND_END_OF_STREAM_IMMEDIATELY,
                    responseCode,
                    blockNumber,
                    lastVerifiedBlockNumber,
                    lastVerifiedBlockConsumer);
        }
    }

    /**
     * Builder for shutting down a specific block node simulator.
     */
    public static class ShutdownBuilder {
        private final int nodeIndex;

        private ShutdownBuilder(int nodeIndex) {
            this.nodeIndex = nodeIndex;
        }

        /**
         * Builds the operation.
         *
         * @return the operation
         */
        public BlockNodeSimulatorOp build() {
            return new BlockNodeSimulatorOp(
                    nodeIndex,
                    BlockNodeSimulatorAction.SHUTDOWN_SIMULATOR,
                    null,
                    0,
                    null,
                    null);
        }
    }

    /**
     * Builder for shutting down all block node simulators.
     */
    public static class ShutdownAllBuilder {
        /**
         * Builds the operation.
         *
         * @return the operation
         */
        public BlockNodeSimulatorOp build() {
            return new BlockNodeSimulatorOp(
                    -1,
                    BlockNodeSimulatorAction.SHUTDOWN_ALL_SIMULATORS,
                    null,
                    0,
                    null,
                    null);
        }
    }

    /**
     * Builder for restarting a specific block node simulator.
     */
    public static class RestartBuilder {
        private final int nodeIndex;

        private RestartBuilder(int nodeIndex) {
            this.nodeIndex = nodeIndex;
        }

        /**
         * Builds the operation.
         *
         * @return the operation
         */
        public BlockNodeSimulatorOp build() {
            return new BlockNodeSimulatorOp(
                    nodeIndex,
                    BlockNodeSimulatorAction.RESTART_SIMULATOR,
                    null,
                    0,
                    null,
                    null);
        }
    }

    /**
     * Builder for restarting all previously shutdown block node simulators.
     */
    public static class RestartAllBuilder {
        /**
         * Builds the operation.
         *
         * @return the operation
         */
        public BlockNodeSimulatorOp build() {
            return new BlockNodeSimulatorOp(
                    -1,
                    BlockNodeSimulatorAction.RESTART_ALL_SIMULATORS,
                    null,
                    0,
                    null,
                    null);
        }
    }
}
