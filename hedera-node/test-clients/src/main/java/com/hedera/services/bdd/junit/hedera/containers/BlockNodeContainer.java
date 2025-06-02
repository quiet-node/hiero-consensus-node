// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.junit.hedera.containers;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A testcontainer for running a block node server instance.
 */
public class BlockNodeContainer extends GenericContainer<BlockNodeContainer> {
    private static final int INTERNAL_PORT = 8080;
    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("ghcr.io/hiero-ledger/hiero-block-node:0.12.0-SNAPSHOT");
    private static final String blockNodeVersion = "0.12.0-SNAPSHOT";

    /**
     * Creates a new block node container with the default image.
     */
    public BlockNodeContainer(long blockNodeId) {
        this(DEFAULT_IMAGE_NAME, blockNodeId);
    }

    /**
     * Creates a new block node container with the specified image.
     *
     * @param dockerImageName the docker image to use
     */
    public BlockNodeContainer(DockerImageName dockerImageName, long blockNodeId) {
        super(dockerImageName);
        withExposedPorts(INTERNAL_PORT);
        withNetworkAliases("block-node-" + blockNodeId);
        withEnv("VERSION", blockNodeVersion);
        waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        waitingFor(Wait.forHealthcheck());
    }

    /**
     * Gets the mapped port for the block node gRPC server.
     *
     * @return the host port mapped to the container's internal port
     */
    public int getGrpcPort() {
        return getMappedPort(INTERNAL_PORT);
    }

    public void waitForHealthy(Duration timeout) {
        waitingFor(Wait.forHealthcheck().withStartupTimeout(timeout));
    }

    @Override
    public String toString() {
        return this.getHost() + ":" + this.getGrpcPort();
    }
}
