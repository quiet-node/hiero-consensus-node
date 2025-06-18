// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.state;

import com.swirlds.base.time.Time;
import com.swirlds.common.merkle.MerkleNode;
import com.swirlds.common.merkle.crypto.MerkleCryptography;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.BinaryState;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.LongSupplier;

/**
 * Represent a state backed up by the Merkle tree. It's a state implementation that is backed by a Merkle tree.
 * It provides methods to manage the service states in the merkle tree.
 */
public interface MerkleNodeState {

    /**
     * Initializes the state with the given parameters.
     * @param time The time provider.
     * @param configuration The platform configuration.
     * @param metrics The metrics provider.
     * @param merkleCryptography The merkle cryptography provider.
     * @param roundSupplier The round supplier.
     */
    void init(
            Time time,
            Configuration configuration,
            Metrics metrics,
            MerkleCryptography merkleCryptography,
            LongSupplier roundSupplier);


    /**
     * @return an instance representing a root of the Merkle tree. For the most of the implementations
     * this default implementation will be sufficient. But some implementations of the state may be "logical" - they
     * are not `MerkleNode` themselves but are backed by the Merkle tree implementation (e.g. a Virtual Map).
     */
    default MerkleNode getRoot() {
        return (MerkleNode) this;
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    MerkleNodeState copy();

    /**
     * Unregister a service without removing its nodes from the state.
     * <p>
     * Services such as the PlatformStateService and RosterService may be registered
     * on a newly loaded (or received via Reconnect) SignedState object in order
     * to access the PlatformState and RosterState/RosterMap objects so that the code
     * can fetch the current active Roster for the state and validate it. Once validated,
     * the state may need to be loaded into the system as the actual state,
     * and as a part of this process, the States API
     * is going to be initialized to allow access to all the services known to the app.
     * However, the States API initialization is guarded by a
     * {@code state.getReadableStates(PlatformStateService.NAME).isEmpty()} check.
     * So if this service has previously been initialized, then the States API
     * won't be initialized in full.
     * <p>
     * To prevent this and to allow the system to initialize all the services,
     * we unregister the PlatformStateService and RosterService after the validation is performed.
     * <p>
     * Note that unlike the {@link #removeServiceState(String, String)} method in this class,
     * the unregisterService() method will NOT remove the merkle nodes that store the states of
     * the services being unregistered. This is by design because these nodes will be used
     * by the actual service states once the app initializes the States API in full.
     *
     * @param serviceName a service to unregister
     */
    void unregisterService(@NonNull final String serviceName);

    /**
     * Removes the node and metadata from the state merkle tree.
     *
     * @param serviceName The service name. Cannot be null.
     * @param stateKey The state key
     */
    void removeServiceState(@NonNull final String serviceName, @NonNull final String stateKey);

    /**
     * Creates a snapshot for the state. The state has to be hashed and immutable before calling this method.
     * @param targetPath The path to save the snapshot.
     */
    default void createSnapshot(final @NonNull Path targetPath) {
        throw new UnsupportedOperationException();
    }

    /**
     * Loads a snapshot of a state.
     * @param targetPath The path to load the snapshot from.
     */
    default MerkleNodeState loadSnapshot(final @NonNull Path targetPath) throws IOException {
        throw new UnsupportedOperationException();
    }
    /**
     * Returns a JSON string containing information about the current state.
     * @return A JSON representation of the state information, or an empty string if no information is available.
     */
    default String getInfoJson() {
        return "";
    }

    BinaryState getBinaryState();
}
