// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.platform.state.NodeId;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import org.hiero.consensus.model.status.PlatformStatus;
import org.hiero.otter.fixtures.result.SingleNodeConsensusResult;
import org.hiero.otter.fixtures.result.SingleNodeLogResult;
import org.hiero.otter.fixtures.result.SingleNodePcesResult;
import org.hiero.otter.fixtures.result.SingleNodePlatformStatusResults;

/**
 * Interface representing a node in the network.
 *
 * <p>This interface provides methods to control the state of the node, such as killing and reviving it.
 */
@SuppressWarnings("unused")
public interface Node {

    /**
     * The default software version of the node when no specific version is set for the node.
     */
    SemanticVersion DEFAULT_VERSION = SemanticVersion.newBuilder().major(1).build();

    /**
     * Kill the node without prior cleanup.
     *
     * <p>This method simulates a sudden failure of the node. No attempt to finish ongoing work,
     * preserve the current state, or any other similar operation is made.
     *
     * <p>The method will wait for a environment-specific timeout before throwing an exception if the nodes cannot be
     * killed. The default can be overridden by calling {@link #withTimeout(Duration)}.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    void killImmediately() throws InterruptedException;

    /**
     * Start the node.
     *
     * <p>The method will wait for a environment-specific timeout before throwing an exception if the node cannot be
     * started. The default can be overridden by calling {@link #withTimeout(Duration)}.
     */
    void start();

    /**
     * Allows to override the default timeout for node operations.
     *
     * @param timeout the duration to wait before considering the operation as failed
     * @return an instance of {@link AsyncNodeActions} that can be used to perform node actions
     */
    AsyncNodeActions withTimeout(@NonNull Duration timeout);

    /**
     * Submit a transaction to the node.
     *
     * @param transaction the transaction to submit
     */
    void submitTransaction(@NonNull byte[] transaction);

    /**
     * Gets the configuration of the node. The returned object can be used to evaluate the current
     * configuration, but also for modifications.
     *
     * @return the configuration of the node
     */
    @NonNull
    NodeConfiguration<?> configuration();

    /**
     * Gets the self id of the node. This value can be used to identify a node.
     *
     * @return the self id
     */
    @NonNull
    NodeId selfId();

    /**
     * Returns the status of the platform while the node is running or {@code null} if not.
     *
     * @return the status of the platform
     */
    @Nullable
    PlatformStatus platformStatus();

    /**
     * Checks if the node's {@link PlatformStatus} is {@link PlatformStatus#ACTIVE}.
     *
     * @return {@code true} if the node is active, {@code false} otherwise
     */
    default boolean isActive() {
        return platformStatus() == PlatformStatus.ACTIVE;
    }

    /**
     * Gets the software version of the node.
     *
     * @return the software version of the node
     */
    @NonNull
    SemanticVersion version();

    /**
     * Sets the software version of the node.
     *
     * <p>If no version is set, {@link #DEFAULT_VERSION} will be used. This method can only be called while the node is not running.
     *
     * @param version the software version to set for the node
     */
    void setVersion(@NonNull SemanticVersion version);

    /**
     * This method updates the version to trigger a "config only upgrade" on the next restart. This method can only be called while the node is not running.
     */
    void bumpConfigVersion();

    /**
     * Gets the consensus rounds of the node.
     *
     * @return the consensus rounds of the node
     */
    @NonNull
    SingleNodeConsensusResult getConsensusResult();

    /**
     * Gets the log results of this node.
     *
     * @return the log results of this node
     */
    @NonNull
    SingleNodeLogResult getLogResult();

    /**
     * Gets the status progression result of the node.
     *
     * @return the status progression result of the node
     */
    @NonNull
    SingleNodePlatformStatusResults getPlatformStatusResults();

    /**
     * Gets the results related to PCES files.
     *
     * @return the PCES files created by the node
     */
    @NonNull
    SingleNodePcesResult getPcesResult();
}
