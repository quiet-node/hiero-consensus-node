// SPDX-License-Identifier: Apache-2.0
package com.swirlds.state.merkle;

import static com.swirlds.logging.legacy.LogMarker.EXCEPTION;
import static com.swirlds.logging.legacy.LogMarker.STARTUP;
import static com.swirlds.state.StateChangeListener.StateType.MAP;
import static com.swirlds.state.StateChangeListener.StateType.QUEUE;
import static com.swirlds.state.StateChangeListener.StateType.SINGLETON;
import static com.swirlds.state.lifecycle.StateMetadata.computeLabel;
import static com.swirlds.state.merkle.StateUtils.decomposeLabel;
import static com.swirlds.state.merkle.StateUtils.getVirtualMapKey;
import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.base.utility.Pair;
import com.swirlds.common.constructable.ConstructableIgnored;
import com.swirlds.common.crypto.DigestType;
import com.swirlds.common.merkle.MerkleInternal;
import com.swirlds.common.merkle.MerkleNode;
import com.swirlds.common.merkle.crypto.MerkleCryptography;
import com.swirlds.common.merkle.impl.PartialNaryMerkleInternal;
import com.swirlds.common.merkle.iterators.MerkleIterator;
import com.swirlds.common.merkle.utility.MerkleTreeSnapshotReader;
import com.swirlds.common.merkle.utility.MerkleTreeSnapshotWriter;
import com.swirlds.common.threading.interrupt.InterruptableConsumer;
import com.swirlds.common.threading.manager.AdHocThreadManager;
import com.swirlds.common.utility.Labeled;
import com.swirlds.common.utility.RuntimeObjectRecord;
import com.swirlds.common.utility.RuntimeObjectRegistry;
import com.swirlds.config.api.Configuration;
import com.swirlds.fcqueue.FCQueue;
import com.swirlds.merkle.map.MerkleMap;
import com.swirlds.merkledb.MerkleDbDataSourceBuilder;
import com.swirlds.merkledb.MerkleDbTableConfig;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.State;
import com.swirlds.state.StateChangeListener;
import com.swirlds.state.lifecycle.StateMetadata;
import com.swirlds.state.merkle.disk.BackedReadableKVState;
import com.swirlds.state.merkle.disk.BackedWritableKVState;
import com.swirlds.state.merkle.memory.InMemoryReadableKVState;
import com.swirlds.state.merkle.memory.InMemoryWritableKVState;
import com.swirlds.state.merkle.queue.BackedReadableQueueState;
import com.swirlds.state.merkle.queue.BackedWritableQueueState;
import com.swirlds.state.merkle.queue.QueueCodec;
import com.swirlds.state.merkle.queue.QueueNode;
import com.swirlds.state.merkle.queue.QueueState;
import com.swirlds.state.merkle.singleton.BackedReadableSingletonState;
import com.swirlds.state.merkle.singleton.BackedWritableSingletonState;
import com.swirlds.state.merkle.singleton.SingletonNode;
import com.swirlds.state.merkle.singleton.StringLeaf;
import com.swirlds.state.merkle.singleton.ValueLeaf;
import com.swirlds.state.spi.CommittableWritableStates;
import com.swirlds.state.spi.EmptyReadableStates;
import com.swirlds.state.spi.KVChangeListener;
import com.swirlds.state.spi.QueueChangeListener;
import com.swirlds.state.spi.ReadableKVState;
import com.swirlds.state.spi.ReadableQueueState;
import com.swirlds.state.spi.ReadableSingletonState;
import com.swirlds.state.spi.ReadableStates;
import com.swirlds.state.spi.WritableKVState;
import com.swirlds.state.spi.WritableKVStateBase;
import com.swirlds.state.spi.WritableQueueState;
import com.swirlds.state.spi.WritableQueueStateBase;
import com.swirlds.state.spi.WritableSingletonState;
import com.swirlds.state.spi.WritableSingletonStateBase;
import com.swirlds.state.spi.WritableStates;
import com.swirlds.virtualmap.VirtualMap;
import com.swirlds.virtualmap.VirtualMapMigration;
import com.swirlds.virtualmap.datasource.VirtualLeafBytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An implementation of {@link State}.
 *
 * <p>Among {@link MerkleStateRoot}'s child nodes are the various {@link
 * com.swirlds.merkle.map.MerkleMap}'s and {@link com.swirlds.virtualmap.VirtualMap}'s that make up
 * the service's states. Each such child node has a label specified that is computed from the
 * metadata for that state. Since both service names and state keys are restricted to characters
 * that do not include the period, we can use it to separate service name from state key. When we
 * need to find all states for a service, we can do so by iteration and string comparison.
 *
 * <p>NOTE: The implementation of this class must change before we can support state proofs
 * properly. In particular, a wide n-ary number of children is less than ideal, since the hash of
 * each child must be part of the state proof. It would be better to have a binary tree. We should
 * consider nesting service nodes in a MerkleMap, or some other such approach to get a binary tree.
 */
@ConstructableIgnored
public abstract class MerkleStateRoot<T extends MerkleStateRoot<T>> extends PartialNaryMerkleInternal
        implements MerkleInternal, State {

    private static final Logger logger = LogManager.getLogger(MerkleStateRoot.class);

    private static final long CLASS_ID = 0x8e300b0dfdafbb1bL;

    // Migrates from `PlatformState` to State API singleton
    public static final int CURRENT_VERSION = 31;

    // This is a temporary fix to deal with the inefficient implementation of findNodeIndex(). It caches looked up
    // indices globally, assuming these indices do not change that often. We need to re-think index lookup,
    // but at this point all major rewrites seem to risky.
    private static final Map<String, Integer> INDEX_LOOKUP = new ConcurrentHashMap<>();
    private LongSupplier roundSupplier;

    private MerkleCryptography merkleCryptography;
    private Time time;

    public Map<String, Map<String, StateMetadata<?, ?>>> getServices() {
        return services;
    }

    private Metrics metrics;

    /**
     * Metrics for the snapshot creation process
     */
    private MerkleRootSnapshotMetrics snapshotMetrics = new MerkleRootSnapshotMetrics();

    /**
     * Maintains information about each service, and each state of each service, known by this
     * instance. The key is the "service-name.state-key".
     */
    private final Map<String, Map<String, StateMetadata<?, ?>>> services = new HashMap<>();

    /**
     * Cache of used {@link ReadableStates}.
     */
    private final Map<String, ReadableStates> readableStatesMap = new ConcurrentHashMap<>();

    /**
     * Cache of used {@link WritableStates}.
     */
    private final Map<String, MerkleWritableStates> writableStatesMap = new HashMap<>();
    /**
     * Listeners to be notified of state changes on {@link MerkleWritableStates#commit()} calls for any service.
     */
    private final List<StateChangeListener> listeners = new ArrayList<>();

    private Configuration configuration;

    /**
     * Used to track the lifespan of this state.
     */
    private final RuntimeObjectRecord registryRecord;

    /**
     * Create a new instance. This constructor must be used for all creations of this class.
     *
     */
    public MerkleStateRoot() {
        this.registryRecord = RuntimeObjectRegistry.createRecord(getClass());
    }

    public void init(
            @NonNull Configuration configuration,
            Time time,
            Metrics metrics,
            MerkleCryptography merkleCryptography,
            LongSupplier roundSupplier) {
        this.configuration = configuration;
        this.time = time;
        this.metrics = metrics;
        this.merkleCryptography = merkleCryptography;
        this.roundSupplier = roundSupplier;
        snapshotMetrics = new MerkleRootSnapshotMetrics(metrics);
    }

    /**
     * Protected constructor for fast-copy.
     *
     * @param from The other state to fast-copy from. Cannot be null.
     */
    protected MerkleStateRoot(@NonNull final MerkleStateRoot<T> from) {
        // Copy the Merkle route from the source instance
        super(from);
        this.registryRecord = RuntimeObjectRegistry.createRecord(getClass());
        this.listeners.addAll(from.listeners);
        this.roundSupplier = from.roundSupplier;

        // Copy over the metadata
        for (final var entry : from.services.entrySet()) {
            this.services.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }

        // Copy the non-null Merkle children from the source (should also be handled by super, TBH).
        // Note we don't "compress" -- null children remain in here unless we manually remove them
        // (which would cause massive re-hashing).
        for (int childIndex = 0, n = from.getNumberOfChildren(); childIndex < n; childIndex++) {
            final var childToCopy = from.getChild(childIndex);
            if (childToCopy != null) {
                setChild(childIndex, childToCopy.copy());
            }
        }
    }

    @Override
    public long getClassId() {
        return CLASS_ID;
    }

    @Override
    public int getVersion() {
        return 32;
    }

    @Override
    public int getMinimumSupportedVersion() {
        return 31;
    }

    /**
     * To be called ONLY at node shutdown. Attempts to gracefully close any virtual maps. This method is a bit of a
     * hack, ideally there would be something more generic at the platform level that virtual maps could hook into
     * to get shutdown in an orderly way.
     */
    public void close() {
        logger.info("Closing MerkleStateRoot");
        for (final var svc : services.values()) {
            for (final var md : svc.values()) {
                final var index = findNodeIndex(md.serviceName(), extractStateKey(md));
                if (index >= 0) {
                    final var node = getChild(index);
                    if (node instanceof VirtualMap virtualMap) {
                        try {
                            virtualMap.getDataSource().close();
                        } catch (IOException e) {
                            logger.warn("Unable to close data source for virtual map {}", md.serviceName(), e);
                        }
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroyNode() {
        registryRecord.release();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public ReadableStates getReadableStates(@NonNull String serviceName) {
        return readableStatesMap.computeIfAbsent(serviceName, s -> {
            final var stateMetadata = services.get(s);
            return stateMetadata == null ? EmptyReadableStates.INSTANCE : new MerkleReadableStates(stateMetadata);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public WritableStates getWritableStates(@NonNull final String serviceName) {
        throwIfImmutable();
        return writableStatesMap.computeIfAbsent(serviceName, s -> {
            final var stateMetadata = services.getOrDefault(s, Map.of());
            return new MerkleWritableStates(serviceName, stateMetadata);
        });
    }

    @Override
    public void registerCommitListener(@NonNull final StateChangeListener listener) {
        requireNonNull(listener);
        listeners.add(listener);
    }

    @Override
    public void unregisterCommitListener(@NonNull final StateChangeListener listener) {
        requireNonNull(listener);
        listeners.remove(listener);
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public T copy() {
        throwIfImmutable();
        throwIfDestroyed();
        setImmutable(true);
        return copyingConstructor();
    }

    protected abstract T copyingConstructor();

    /**
     * Puts the defined service state and its associated node into the merkle tree. The precondition
     * for calling this method is that node MUST be a {@link MerkleMap} or {@link VirtualMap} and
     * MUST have a correct label applied. No matter if the resulting node is newly created or already
     * present, calls the provided initialization consumer with the node.
     *
     * @param md The metadata associated with the state
     * @param nodeSupplier Returns the node to add. Cannot be null. Can be used to create the node on-the-fly.
     * @param nodeInitializer The node's initialization logic.
     * @throws IllegalArgumentException if the node is neither a merkle map nor virtual map, or if
     * it doesn't have a label, or if the label isn't right.
     */
    public <T extends MerkleNode> void putServiceStateIfAbsent(
            @NonNull final StateMetadata<?, ?> md,
            @NonNull final Supplier<T> nodeSupplier,
            @NonNull final Consumer<T> nodeInitializer) {

        logger.info(STARTUP.getMarker(), "Putting states... ", md.serviceName());

        // Validate the inputs
        throwIfImmutable();
        requireNonNull(md);
        requireNonNull(nodeSupplier);
        requireNonNull(nodeInitializer);

        // Put this metadata into the map
        final var def = md.stateDefinition();
        final var serviceName = md.serviceName();
        final var stateMetadata = services.computeIfAbsent(serviceName, k -> new HashMap<>());
        stateMetadata.put(def.stateKey(), md);

        // We also need to add/update the metadata of the service in the writableStatesMap so that
        // it isn't stale or incomplete (e.g. in a genesis case)
        readableStatesMap.put(serviceName, new MerkleReadableStates(stateMetadata));
        writableStatesMap.put(serviceName, new MerkleWritableStates(serviceName, stateMetadata));

        logger.info(STARTUP.getMarker(), "Put states! Service name: {} ", md.serviceName());

        // Look for a node, and if we don't find it, then insert the one we were given
        // If there is not a node there, then set it. I don't want to overwrite the existing node,
        // because it may have been loaded from state on disk, and the node provided here in this
        // call is always for genesis. So we may just ignore it.
        final T node;
        final var nodeIndex = findNodeIndex(serviceName, def.stateKey());
        if (nodeIndex == -1) {
            node = requireNonNull(nodeSupplier.get());
            final var label = node instanceof Labeled labeled ? labeled.getLabel() : null;
            if (label == null) {
                throw new IllegalArgumentException("`node` must be a Labeled and have a label");
            }

            if (def.onDisk() && !(node instanceof VirtualMap)) {
                throw new IllegalArgumentException(
                        "Mismatch: state definition claims on-disk, but " + "the merkle node is not a VirtualMap");
            }

            if (label.isEmpty()) {
                // It looks like both MerkleMap and VirtualMap do not allow for a null label.
                // But I want to leave this check in here anyway, in case that is ever changed.
                throw new IllegalArgumentException("A label must be specified on the node");
            }

            if (!label.equals(computeLabel(serviceName, def.stateKey()))) {
                throw new IllegalArgumentException(
                        "A label must be computed based on the same " + "service name and state key in the metadata!");
            }

            logger.info(
                    STARTUP.getMarker(),
                    "Setting child.. Service name: {} / Number of children: {} / node: {}",
                    md.serviceName(),
                    getNumberOfChildren(),
                    node);
            setChild(getNumberOfChildren(), node);
        } else {
            logger.info(
                    STARTUP.getMarker(),
                    "Getting child.. Service name: {} / Number of children: {} / node: {}",
                    md.serviceName(),
                    getNumberOfChildren(),
                    nodeIndex);

            node = getChild(nodeIndex);
        }
        nodeInitializer.accept(node);
    }

    /**
     * Unregister a service without removing its nodes from the state.
     *
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
     *
     * To prevent this and to allow the system to initialize all the services,
     * we unregister the PlatformStateService and RosterService after the validation is performed.
     *
     * Note that unlike the MerkleStateRoot.removeServiceState() method below in this class,
     * the unregisterService() method will NOT remove the merkle nodes that store the states of
     * the services being unregistered. This is by design because these nodes will be used
     * by the actual service states once the app initializes the States API in full.
     *
     * @param serviceName a service to unregister
     */
    public void unregisterService(@NonNull final String serviceName) {
        readableStatesMap.remove(serviceName);
        writableStatesMap.remove(serviceName);

        services.remove(serviceName);
    }

    /**
     * Removes the node and metadata from the state merkle tree.
     *
     * @param serviceName The service name. Cannot be null.
     * @param stateKey The state key
     */
    public void removeServiceState(@NonNull final String serviceName, @NonNull final String stateKey) {
        throwIfImmutable();
        requireNonNull(serviceName);
        requireNonNull(stateKey);

        // Remove the metadata entry
        final var stateMetadata = services.get(serviceName);
        if (stateMetadata != null) {
            stateMetadata.remove(stateKey);
        }

        // Eventually remove the cached WritableState
        final var writableStates = writableStatesMap.get(serviceName);
        if (writableStates != null) {
            writableStates.remove(stateKey);
        }

        // Remove the node
        final var index = findNodeIndex(serviceName, stateKey);
        if (index != -1) {
            setChild(index, null);
        }
    }

    /**
     * Simple utility method that finds the state node index.
     *
     * @param serviceName the service name
     * @param stateKey the state key
     * @return -1 if not found, otherwise the index into the children
     */
    public int findNodeIndex(@NonNull final String serviceName, @NonNull final String stateKey) {
        final var label = computeLabel(serviceName, stateKey);

        final Integer index = INDEX_LOOKUP.get(label);
        if (index != null && checkNodeIndex(index, label)) {
            return index;
        }

        for (int i = 0, n = getNumberOfChildren(); i < n; i++) {
            if (checkNodeIndex(i, label)) {
                INDEX_LOOKUP.put(label, i);
                return i;
            }
        }

        INDEX_LOOKUP.remove(label);
        return -1;
    }

    private boolean checkNodeIndex(final int index, @NonNull final String label) {
        final var node = getChild(index);
        return node instanceof Labeled labeled && Objects.equals(label, labeled.getLabel());
    }

    /**
     * Base class implementation for states based on MerkleTree
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private abstract class MerkleStates implements ReadableStates {
        protected final Map<String, StateMetadata<?, ?>> stateMetadata;
        protected final Map<String, ReadableKVState<?, ?>> kvInstances;
        protected final Map<String, ReadableSingletonState<?>> singletonInstances;
        protected final Map<String, ReadableQueueState<?>> queueInstances;
        private final Set<String> stateKeys;

        /**
         * Create a new instance
         *
         * @param stateMetadata cannot be null
         */
        MerkleStates(@NonNull final Map<String, StateMetadata<?, ?>> stateMetadata) {
            this.stateMetadata = requireNonNull(stateMetadata);
            this.stateKeys = Collections.unmodifiableSet(stateMetadata.keySet());
            this.kvInstances = new HashMap<>();
            this.singletonInstances = new HashMap<>();
            this.queueInstances = new HashMap<>();
        }

        @NonNull
        @Override
        public <K, V> ReadableKVState<K, V> get(@NonNull String stateKey) {
            final ReadableKVState<K, V> instance = (ReadableKVState<K, V>) kvInstances.get(stateKey);
            if (instance != null) {
                return instance;
            }

            final var md = stateMetadata.get(stateKey);
            if (md == null || md.stateDefinition().singleton()) {
                throw new IllegalArgumentException("Unknown k/v state key '" + stateKey + ";");
            }

            final var node = findNode(md);
            if (node instanceof VirtualMap v) {
                final var ret = createReadableKVState(md, v);
                kvInstances.put(stateKey, ret);
                return ret;
            } else if (node instanceof MerkleMap m) {
                final var ret = createReadableKVState(md, m);
                kvInstances.put(stateKey, ret);
                return ret;
            } else {
                // This exception should never be thrown. Only if "findNode" found the wrong node!
                throw new IllegalStateException("Unexpected type for k/v state " + stateKey);
            }
        }

        @NonNull
        @Override
        public <S> ReadableSingletonState<S> getSingleton(@NonNull String stateKey) {
            final ReadableSingletonState<S> instance = (ReadableSingletonState<S>) singletonInstances.get(stateKey);
            if (instance != null) {
                return instance;
            }

            final var md = stateMetadata.get(stateKey);
            if (md == null || !md.stateDefinition().singleton()) {
                throw new IllegalArgumentException("Unknown singleton state key '" + stateKey + "'");
            }

            final var node = findNode(md);
            if (node instanceof SingletonNode s) {
                final var ret = createReadableSingletonState(md, s);
                singletonInstances.put(stateKey, ret);
                return ret;
            } else {
                // This exception should never be thrown. Only if "findNode" found the wrong node!
                throw new IllegalStateException("Unexpected type for singleton state " + stateKey);
            }
        }

        @NonNull
        @Override
        public <E> ReadableQueueState<E> getQueue(@NonNull String stateKey) {
            final ReadableQueueState<E> instance = (ReadableQueueState<E>) queueInstances.get(stateKey);
            if (instance != null) {
                return instance;
            }

            final var md = stateMetadata.get(stateKey);
            if (md == null || !md.stateDefinition().queue()) {
                throw new IllegalArgumentException("Unknown queue state key '" + stateKey + "'");
            }

            final var node = findNode(md);
            if (node instanceof QueueNode q) {
                final var ret = createReadableQueueState(md, q);
                queueInstances.put(stateKey, ret);
                return ret;
            } else {
                // This exception should never be thrown. Only if "findNode" found the wrong node!
                throw new IllegalStateException("Unexpected type for queue state " + stateKey);
            }
        }

        @Override
        public boolean contains(@NonNull final String stateKey) {
            return stateMetadata.containsKey(stateKey);
        }

        @NonNull
        @Override
        public Set<String> stateKeys() {
            return stateKeys;
        }

        @NonNull
        protected abstract ReadableKVState createReadableKVState(@NonNull StateMetadata md, @NonNull VirtualMap v);

        @NonNull
        protected abstract ReadableKVState createReadableKVState(@NonNull StateMetadata md, @NonNull MerkleMap m);

        @NonNull
        protected abstract ReadableSingletonState createReadableSingletonState(
                @NonNull StateMetadata md, @NonNull SingletonNode<?> s);

        @NonNull
        protected abstract ReadableQueueState createReadableQueueState(
                @NonNull StateMetadata md, @NonNull QueueNode<?> q);

        /**
         * Utility method for finding and returning the given node. Will throw an ISE if such a node
         * cannot be found!
         *
         * @param md The metadata
         * @return The found node
         */
        @NonNull
        MerkleNode findNode(@NonNull final StateMetadata<?, ?> md) {
            final var index = findNodeIndex(md.serviceName(), extractStateKey(md));
            if (index == -1) {
                // This can only happen if there WAS a node here, and it was removed!
                throw new IllegalStateException("State '"
                        + extractStateKey(md)
                        + "' for service '"
                        + md.serviceName()
                        + "' is missing from the merkle tree!");
            }

            return getChild(index);
        }
    }

    /**
     * An implementation of {@link ReadableStates} based on the merkle tree.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public final class MerkleReadableStates extends MerkleStates {
        /**
         * Create a new instance
         *
         * @param stateMetadata cannot be null
         */
        MerkleReadableStates(@NonNull final Map<String, StateMetadata<?, ?>> stateMetadata) {
            super(stateMetadata);
        }

        @Override
        @NonNull
        protected ReadableKVState<?, ?> createReadableKVState(
                @NonNull final StateMetadata md, @NonNull final VirtualMap v) {
            return new BackedReadableKVState<>(
                    md.serviceName(),
                    extractStateKey(md),
                    Objects.requireNonNull(md.stateDefinition().keyCodec()),
                    md.stateDefinition().valueCodec(),
                    v);
        }

        @Override
        @NonNull
        protected ReadableKVState<?, ?> createReadableKVState(
                @NonNull final StateMetadata md, @NonNull final MerkleMap m) {
            return new InMemoryReadableKVState<>(md.serviceName(), extractStateKey(md), m);
        }

        @Override
        @NonNull
        protected ReadableSingletonState<?> createReadableSingletonState(
                @NonNull final StateMetadata md, @NonNull final SingletonNode<?> s) {
            return new BackedReadableSingletonState<>(md.serviceName(), extractStateKey(md), s);
        }

        @NonNull
        @Override
        protected ReadableQueueState createReadableQueueState(@NonNull StateMetadata md, @NonNull QueueNode<?> q) {
            return new BackedReadableQueueState<>(md.serviceName(), extractStateKey(md), q);
        }
    }

    /**
     * An implementation of {@link WritableStates} based on the merkle tree.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public final class MerkleWritableStates extends MerkleStates implements WritableStates, CommittableWritableStates {

        private final String serviceName;

        /**
         * Create a new instance
         *
         * @param stateMetadata cannot be null
         */
        MerkleWritableStates(
                @NonNull final String serviceName, @NonNull final Map<String, StateMetadata<?, ?>> stateMetadata) {
            super(stateMetadata);
            this.serviceName = requireNonNull(serviceName);
        }

        /**
         * Copies and releases the {@link VirtualMap} for the given state key. This ensures
         * data is continually flushed to disk
         *
         * @param stateKey the state key
         */
        public void copyAndReleaseVirtualMap(@NonNull final String stateKey) {
            final var md = stateMetadata.get(stateKey);
            final VirtualMap virtualMap = findNode(md).cast();
            final var mutableCopy = virtualMap.copy();
            if (metrics != null) {
                mutableCopy.registerMetrics(metrics);
            }
            setChild(findNodeIndex(serviceName, stateKey), mutableCopy);
            kvInstances.put(stateKey, createReadableKVState(md, mutableCopy));
        }

        @NonNull
        @Override
        public <K, V> WritableKVState<K, V> get(@NonNull String stateKey) {
            return (WritableKVState<K, V>) super.get(stateKey);
        }

        @NonNull
        @Override
        public <S> WritableSingletonState<S> getSingleton(@NonNull String stateKey) {
            return (WritableSingletonState<S>) super.getSingleton(stateKey);
        }

        @NonNull
        @Override
        public <E> WritableQueueState<E> getQueue(@NonNull String stateKey) {
            return (WritableQueueState<E>) super.getQueue(stateKey);
        }

        @Override
        @NonNull
        protected WritableKVState<?, ?> createReadableKVState(
                @NonNull final StateMetadata md, @NonNull final VirtualMap v) {
            final var state = new BackedWritableKVState<Object, Object>(
                    serviceName,
                    extractStateKey(md),
                    Objects.requireNonNull(md.stateDefinition().keyCodec()),
                    md.stateDefinition().valueCodec(),
                    v);
            listeners.forEach(listener -> {
                if (listener.stateTypes().contains(MAP)) {
                    registerKVListener(serviceName, state, listener);
                }
            });
            return state;
        }

        @Override
        @NonNull
        protected WritableKVState<?, ?> createReadableKVState(
                @NonNull final StateMetadata md, @NonNull final MerkleMap m) {
            final var state = new InMemoryWritableKVState<>(
                    md.serviceName(),
                    extractStateKey(md),
                    md.inMemoryValueClassId(),
                    md.stateDefinition().keyCodec(),
                    md.stateDefinition().valueCodec(),
                    m);
            listeners.forEach(listener -> {
                if (listener.stateTypes().contains(MAP)) {
                    registerKVListener(serviceName, state, listener);
                }
            });
            return state;
        }

        @Override
        @NonNull
        protected WritableSingletonState<?> createReadableSingletonState(
                @NonNull final StateMetadata md, @NonNull final SingletonNode<?> s) {
            final var state = new BackedWritableSingletonState<>(md.serviceName(), extractStateKey(md), s);
            listeners.forEach(listener -> {
                if (listener.stateTypes().contains(SINGLETON)) {
                    registerSingletonListener(serviceName, state, listener);
                }
            });
            return state;
        }

        @NonNull
        @Override
        protected WritableQueueState<?> createReadableQueueState(
                @NonNull final StateMetadata md, @NonNull final QueueNode<?> q) {
            final var state = new BackedWritableQueueState<>(md.serviceName(), extractStateKey(md), q);
            listeners.forEach(listener -> {
                if (listener.stateTypes().contains(QUEUE)) {
                    registerQueueListener(serviceName, state, listener);
                }
            });
            return state;
        }

        @Override
        public void commit() {
            for (final ReadableKVState kv : kvInstances.values()) {
                ((WritableKVStateBase) kv).commit();
            }
            for (final ReadableSingletonState s : singletonInstances.values()) {
                ((WritableSingletonStateBase) s).commit();
            }
            for (final ReadableQueueState q : queueInstances.values()) {
                ((WritableQueueStateBase) q).commit();
            }
            readableStatesMap.remove(serviceName);
        }

        /**
         * This method is called when a state is removed from the state merkle tree. It is used to
         * remove the cached instances of the state.
         *
         * @param stateKey the state key
         */
        public void remove(String stateKey) {
            if (!Map.of().equals(stateMetadata)) {
                stateMetadata.remove(stateKey);
            }
            kvInstances.remove(stateKey);
            singletonInstances.remove(stateKey);
            queueInstances.remove(stateKey);
        }

        private <V> void registerSingletonListener(
                @NonNull final String serviceName,
                @NonNull final WritableSingletonStateBase<V> singletonState,
                @NonNull final StateChangeListener listener) {
            final var stateId = listener.stateIdFor(serviceName, singletonState.getStateKey());
            singletonState.registerListener(value -> listener.singletonUpdateChange(stateId, value));
        }

        private <V> void registerQueueListener(
                @NonNull final String serviceName,
                @NonNull final WritableQueueStateBase<V> queueState,
                @NonNull final StateChangeListener listener) {
            final var stateId = listener.stateIdFor(serviceName, queueState.getStateKey());
            queueState.registerListener(new QueueChangeListener<>() {
                @Override
                public void queuePushChange(@NonNull final V value) {
                    listener.queuePushChange(stateId, value);
                }

                @Override
                public void queuePopChange() {
                    listener.queuePopChange(stateId);
                }
            });
        }

        private <K, V> void registerKVListener(
                @NonNull final String serviceName, WritableKVStateBase<K, V> state, StateChangeListener listener) {
            final var stateId = listener.stateIdFor(serviceName, state.getStateKey());
            state.registerListener(new KVChangeListener<>() {
                @Override
                public void mapUpdateChange(@NonNull final K key, @NonNull final V value) {
                    listener.mapUpdateChange(stateId, key, value);
                }

                @Override
                public void mapDeleteChange(@NonNull final K key) {
                    listener.mapDeleteChange(stateId, key);
                }
            });
        }
    }

    @NonNull
    private static String extractStateKey(@NonNull final StateMetadata<?, ?> md) {
        return md.stateDefinition().stateKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void computeHash() {
        requireNonNull(
                merkleCryptography,
                "MerkleStateRoot has to be initialized before hashing. merkleCryptography is not set.");
        throwIfMutable("Hashing should only be done on immutable states");
        throwIfDestroyed("Hashing should not be done on destroyed states");
        if (getHash() != null) {
            return;
        }
        try {
            merkleCryptography.digestTreeAsync(this).get();
        } catch (final ExecutionException e) {
            logger.error(EXCEPTION.getMarker(), "Exception occurred during hashing", e);
        } catch (final InterruptedException e) {
            logger.error(EXCEPTION.getMarker(), "Interrupted while hashing state. Expect buggy behavior.");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createSnapshot(@NonNull final Path targetPath) {
        requireNonNull(time);
        requireNonNull(snapshotMetrics);
        throwIfMutable();
        throwIfDestroyed();
        final long startTime = time.currentTimeMillis();
        MerkleTreeSnapshotWriter.createSnapshot(this, targetPath, roundSupplier.getAsLong());
        snapshotMetrics.updateWriteStateToDiskTimeMetric(time.currentTimeMillis() - startTime);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T loadSnapshot(@NonNull Path targetPath) throws IOException {
        requireNonNull(configuration);
        return (T) MerkleTreeSnapshotReader.readStateFileData(configuration, targetPath)
                .stateRoot();
    }

    // END * MOSTLY * DEPRECATED CODE

    // START MIGRATION CODE

    // TODO: double check assert usage

    // Config constants (TODO: move to config)
    // Threads which iterate over the given Virtual Map, perform some operation and write into its own output
    // queue/buffer
    private static final int THREAD_COUNT = 1;
    private static final long MEGA_MAP_MAX_KEYS_HINT = 1_000_000_000;
    private static final int DATA_PER_COPY = 10_213;
    private static final boolean VALIDATE_MIGRATION_FF = true;

    @Override
    public MerkleNode migrate(@NonNull final Configuration configuration, int version) {
        if (version < 32) {

            // Create Virtual Map

            final MerkleDbConfig merkleDbConfig = configuration.getConfigData(MerkleDbConfig.class);
            final var tableConfig = new MerkleDbTableConfig(
                    (short) 1, DigestType.SHA_384, MEGA_MAP_MAX_KEYS_HINT, merkleDbConfig.hashesRamToDiskThreshold());
            final var virtualMapLabel = "VirtualMap";
            final var dsBuilder = new MerkleDbDataSourceBuilder(tableConfig, configuration);
            final var virtualMap = new VirtualMap(virtualMapLabel, dsBuilder, configuration);

            // Initialize migration metrics

            AtomicLong totalMigratedObjects = new AtomicLong(0);
            AtomicLong totalMigrationTimeMs = new AtomicLong(0);
            AtomicLong totalValidationTimeMs = new AtomicLong(0);

            // Migration

            logger.info(
                    STARTUP.getMarker(),
                    "Migrating all of the states (Singleton, KV and Queue) to the one Virtual Map...");

            migrateSingletonStates(virtualMap, totalMigratedObjects, totalMigrationTimeMs, totalValidationTimeMs);

            final AtomicReference<VirtualMap> virtualMapRef = new AtomicReference<>(virtualMap);
            migrateQueueStates(virtualMapRef, totalMigratedObjects, totalMigrationTimeMs, totalValidationTimeMs);
            migrateKVStates(virtualMapRef, totalMigratedObjects, totalMigrationTimeMs, totalValidationTimeMs);

            logger.info(STARTUP.getMarker(), "Total migration time {} ms", totalMigrationTimeMs.get());

            // Validate all states migrated to the Virtual Map
            if (VALIDATE_MIGRATION_FF) {
                assert virtualMapRef.get().size() == totalMigratedObjects.get();
                logger.info(STARTUP.getMarker(), "Total validation time {} ms", totalValidationTimeMs.get());
            }

            return virtualMapRef.get();
        }

        return this;
    }

    private void migrateKVStates(
            final AtomicReference<VirtualMap> virtualMapRef,
            AtomicLong totalMigratedObjects,
            AtomicLong totalMigrationTimeMs,
            AtomicLong totalValidationTimeMs) {
        logger.info(STARTUP.getMarker(), "Migrating KV states to the one Virtual Map...");

        final AtomicLong kvMigrationStartTime = new AtomicLong(0);
        IntStream.range(0, getNumberOfChildren())
                .mapToObj(this::getChild)
                .filter(child -> child instanceof VirtualMap)
                .map(child -> (VirtualMap) child)
                .forEach(virtualMapToMigrate -> {
                    final var virtualMapLabel = virtualMapToMigrate.getLabel();
                    final var labelPair = decomposeLabel(virtualMapToMigrate.getLabel());
                    final var serviceName = labelPair.key();
                    final var stateKey = labelPair.value();
                    final var stateIdBytes = getVirtualMapKey(serviceName, stateKey);

                    // TODO: check possibilities for optimization
                    InterruptableConsumer<Pair<Bytes, Bytes>> handler = (pair) -> {
                        VirtualMap currentMap = virtualMapRef.get();
                        if (currentMap.size() % DATA_PER_COPY == 0) {
                            VirtualMap older = currentMap;
                            currentMap = currentMap.copy();
                            older.release();
                            virtualMapRef.set(currentMap);
                        }
                        virtualMapRef.get().putBytes(stateIdBytes.append(pair.key()), pair.value());
                    };

                    try {
                        logger.info(
                                STARTUP.getMarker(),
                                "\nMigrating {} (size: {})...",
                                virtualMapLabel,
                                virtualMapToMigrate.size());
                        long migrationStartTime = System.currentTimeMillis();

                        // TODO: decide on method from VirtualMapMigration
                        VirtualMapMigration.extractVirtualMapData(
                                AdHocThreadManager.getStaticThreadManager(),
                                virtualMapToMigrate,
                                handler,
                                THREAD_COUNT);

                        long migrationTimeMs = System.currentTimeMillis() - migrationStartTime;
                        logger.info(
                                STARTUP.getMarker(),
                                "Migration complete for {} took {} ms",
                                virtualMapLabel,
                                migrationTimeMs);
                        logger.info(
                                STARTUP.getMarker(),
                                "New Virtual Map size: {}",
                                virtualMapRef.get().size());
                        kvMigrationStartTime.addAndGet(migrationTimeMs);
                        totalMigrationTimeMs.addAndGet(migrationTimeMs);
                        totalMigratedObjects.addAndGet(virtualMapToMigrate.size());
                    } catch (InterruptedException e) { // TODO: revisit exception handling
                        throw new RuntimeException(e);
                    }

                    if (VALIDATE_MIGRATION_FF) {
                        long validationStartTime = System.currentTimeMillis();
                        logger.info(
                                STARTUP.getMarker(),
                                "Validating the new Virtual Map contains all data from the KV State {}",
                                virtualMapToMigrate.getLabel());

                        validateKVStateMigrated(virtualMapRef.get(), virtualMapToMigrate);

                        long validationTimeMs = System.currentTimeMillis() - validationStartTime;
                        logger.info(
                                STARTUP.getMarker(),
                                "Validation complete for the KV State {} took {} ms",
                                virtualMapToMigrate.getLabel(),
                                validationTimeMs);
                        totalValidationTimeMs.addAndGet(validationTimeMs);
                    }
                });

        logger.info(STARTUP.getMarker(), "Migration complete for KV states, took {} ms", kvMigrationStartTime.get());
    }

    private static void validateKVStateMigrated(VirtualMap virtualMap, VirtualMap virtualMapToMigrate) {
        MerkleIterator<MerkleNode> merkleNodeMerkleIterator = virtualMapToMigrate.treeIterator();

        while (merkleNodeMerkleIterator.hasNext()) {
            MerkleNode next = merkleNodeMerkleIterator.next();
            if (next instanceof VirtualLeafBytes leafBytes) { // TODO: double check this
                assert virtualMap.containsKey(leafBytes.keyBytes());
            }
        }
    }

    private void migrateQueueStates(
            final AtomicReference<VirtualMap> virtualMapRef,
            AtomicLong totalMigratedObjects,
            AtomicLong totalMigrationTimeMs,
            AtomicLong totalValidationTimeMs) {
        logger.info(STARTUP.getMarker(), "Migrating Queue states to the one Virtual Map...");

        final AtomicLong queueMigrationStartTime = new AtomicLong(0);
        IntStream.range(0, getNumberOfChildren())
                .mapToObj(this::getChild)
                .filter(child -> child instanceof QueueNode<?>)
                .map(child -> (QueueNode<?>) child)
                .forEach(queueNode -> {
                    final var queueNodeLabel = queueNode.getLabel();
                    final var labelPair = decomposeLabel(queueNodeLabel);
                    final var serviceName = labelPair.key();
                    final var stateKey = labelPair.value();
                    final FCQueue<ValueLeaf> originalStore = queueNode.getRight();

                    logger.info(
                            STARTUP.getMarker(), "\nMigrating {} (size: {})...", queueNodeLabel, originalStore.size());
                    long migrationStartTime = System.currentTimeMillis();

                    // Migrate data
                    final long head = 1;
                    long tail = 1;

                    for (ValueLeaf leaf : originalStore) {
                        final var codec = leaf.getCodec();
                        final var value = Objects.requireNonNull(leaf.getValue(), "Null value is not expected here");

                        VirtualMap currentMap = virtualMapRef.get();
                        if (currentMap.size() % DATA_PER_COPY == 0) {
                            VirtualMap older = currentMap;
                            currentMap = currentMap.copy();
                            older.release();
                            virtualMapRef.set(currentMap);
                        }
                        virtualMapRef.get().put(getVirtualMapKey(serviceName, stateKey, tail++), value, codec);
                    }

                    final var queueState = new QueueState(head, tail);
                    virtualMapRef.get().put(getVirtualMapKey(serviceName, stateKey), queueState, QueueCodec.INSTANCE);

                    long migrationTimeMs = System.currentTimeMillis() - migrationStartTime;
                    logger.info(
                            STARTUP.getMarker(),
                            "Migration complete for {} took {} ms",
                            queueNodeLabel,
                            migrationTimeMs);
                    logger.info(
                            STARTUP.getMarker(),
                            "New Virtual Map size: {}",
                            virtualMapRef.get().size());
                    queueMigrationStartTime.addAndGet(migrationTimeMs);
                    totalMigrationTimeMs.addAndGet(migrationTimeMs);
                    totalMigratedObjects.addAndGet(originalStore.size());

                    if (VALIDATE_MIGRATION_FF) {
                        long validationStartTime = System.currentTimeMillis();
                        logger.info(
                                STARTUP.getMarker(),
                                "Validating the new Virtual Map contains all data from the Queue State {}",
                                queueNodeLabel);

                        validateQueueStateMigrated(virtualMapRef.get(), queueNodeLabel, serviceName, head, tail);

                        long validationTimeMs = System.currentTimeMillis() - validationStartTime;
                        logger.info(
                                STARTUP.getMarker(),
                                "Validation complete for the Queue State {} took {} ms",
                                queueNodeLabel,
                                validationTimeMs);
                        totalValidationTimeMs.addAndGet(validationTimeMs);
                    }
                });

        logger.info(
                STARTUP.getMarker(), "Migration complete for Queue states, took {} ms", queueMigrationStartTime.get());
    }

    private static void validateQueueStateMigrated(
            VirtualMap virtualMap, String serviceName, String stateKey, long head, long tail) {
        // Validate Queue State object
        assert virtualMap.containsKey(getVirtualMapKey(serviceName, stateKey));

        // Validate Queue State values
        for (long i = head; i < tail; i++) {
            assert virtualMap.containsKey(getVirtualMapKey(serviceName, stateKey, i));
        }
    }

    private void migrateSingletonStates(
            VirtualMap virtualMap,
            AtomicLong totalMigratedObjects,
            AtomicLong totalMigrationTimeMs,
            AtomicLong totalValidationTimeMs) {
        logger.info(STARTUP.getMarker(), "Migrating Singleton states to the one Virtual Map...");

        final AtomicLong singletonMigrationTimeMs = new AtomicLong(0);
        IntStream.range(0, getNumberOfChildren())
                .mapToObj(this::getChild)
                .filter(child -> child instanceof SingletonNode<?>)
                .map(child -> (SingletonNode<?>) child)
                .forEach(singletonNode -> {
                    final StringLeaf originalLabeled = singletonNode.getLeft();
                    final String singletonStateLabel = originalLabeled.getLabel();
                    final var labelPair = decomposeLabel(singletonStateLabel);
                    final var serviceName = labelPair.key();
                    final var stateKey = labelPair.value();
                    final ValueLeaf originalStore = singletonNode.getRight();

                    logger.info(STARTUP.getMarker(), "\nMigrating {}...", singletonStateLabel);
                    long migrationStartTime = System.currentTimeMillis();

                    final var codec = originalStore.getCodec();
                    final var value =
                            Objects.requireNonNull(originalStore.getValue(), "Null value is not expected here");
                    virtualMap.put(getVirtualMapKey(serviceName, stateKey), value, codec);

                    long migrationTimeMs = System.currentTimeMillis() - migrationStartTime;
                    logger.info(
                            STARTUP.getMarker(),
                            "Migration complete for {} took {} ms",
                            singletonStateLabel,
                            migrationTimeMs);
                    logger.info(STARTUP.getMarker(), "New Virtual Map size: {}", virtualMap.size());
                    singletonMigrationTimeMs.addAndGet(migrationTimeMs);
                    totalMigrationTimeMs.addAndGet(migrationTimeMs);
                    totalMigratedObjects.addAndGet(1);

                    if (VALIDATE_MIGRATION_FF) {
                        long validationStartTime = System.currentTimeMillis();
                        logger.info(
                                STARTUP.getMarker(),
                                "Validating the new Virtual Map contains all data from the Singleton State {}",
                                singletonStateLabel);

                        validateSingletonStateMigrated(virtualMap, serviceName, stateKey);

                        final long validationTimeMs = System.currentTimeMillis() - validationStartTime;
                        logger.info(
                                STARTUP.getMarker(),
                                "Validation complete for the Singleton State {} took {} ms",
                                singletonStateLabel,
                                validationTimeMs);
                        totalValidationTimeMs.addAndGet(validationTimeMs);
                    }
                });

        logger.info(
                STARTUP.getMarker(), "Migration complete for Singleton states, took {} ms", singletonMigrationTimeMs);
    }

    private static void validateSingletonStateMigrated(VirtualMap virtualMap, String serviceName, String stateKey) {
        assert virtualMap.containsKey(getVirtualMapKey(serviceName, stateKey));
    }
}
