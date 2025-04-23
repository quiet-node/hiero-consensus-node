// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.hints.impl;

import static com.hedera.node.app.roster.RosterTransitionWeights.atLeastOneThirdOfTotal;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

import com.hedera.hapi.node.state.hints.HintsConstruction;
import com.hedera.hapi.node.state.hints.NodePartyId;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.node.state.roster.RosterEntry;
import com.hedera.hapi.services.auxiliary.hints.HintsPartialSignatureTransactionBody;
import com.hedera.node.app.hints.HintsLibrary;
import com.hedera.node.app.spi.AppContext;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.lifecycle.info.NodeInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The hinTS context that can be used to request hinTS signatures using the latest
 * complete construction, if there is one. See {@link #setActiveConstruction(HintsConstruction)}
 * for the ways the context can have a construction set.
 */
@Singleton
public class HintsContext {
    private static final Logger log = LogManager.getLogger(HintsContext.class);

    private static final Duration SIGNING_ATTEMPT_TIMEOUT = Duration.ofSeconds(30);

    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private final Executor executor;

    private final HintsLibrary library;

    private final Supplier<NodeInfo> selfNodeInfoSupplier;

    @Nullable
    private Bytes crs;

    @Nullable
    private HintsConstruction construction;

    @Nullable
    private Map<Long, Integer> nodePartyIds;

    private long schemeId;

    @Inject
    public HintsContext(
            @NonNull final HintsLibrary library,
            @NonNull final Executor executor,
            @NonNull final AppContext appContext) {
        this.library = requireNonNull(library);
        this.executor = requireNonNull(executor);
        this.selfNodeInfoSupplier = requireNonNull(appContext).selfNodeInfoSupplier();
    }

    /**
     * Sets the active hinTS construction as the signing context. Called in three places,
     * <ol>
     *     <li>In the startup phase, when restarting from a state whose active hinTS
     *     construction (and possibly next construction) had complete schemes.</li>
     *     <li>In the bootstrap runtime phase, on finishing the preprocessing work for
     *     the genesis hinTS construction.</li>
     *     <li>In the restart runtime phase, when swapping in a newly adopted roster's
     *     hinTS construction and purging votes for the previous construction.</li>
     * </ol>
     *
     * @param activeConstruction the active construction
     * @throws IllegalArgumentException if either construction does not have a hinTS scheme
     */
    public void setActiveConstruction(@NonNull final HintsConstruction activeConstruction) {
        requireNonNull(activeConstruction);
        if (!activeConstruction.hasHintsScheme()) {
            throw new IllegalArgumentException(
                    "Active construction #" + activeConstruction.constructionId() + " has no hinTS scheme");
        }
        construction = requireNonNull(activeConstruction);
        nodePartyIds = asNodePartyIds(activeConstruction.hintsSchemeOrThrow().nodePartyIds());
        schemeId = construction.constructionId();
    }

    /**
     * Sets the final CRS used by the network.
     *
     * @param crs the final CRS
     */
    public void setCrs(@NonNull final Bytes crs) {
        requireNonNull(crs);
        this.crs = crs;
    }

    /**
     * Returns true if the signing context is ready.
     *
     * @return true if the context is ready
     */
    public boolean isReady() {
        return construction != null && construction.hasHintsScheme();
    }

    /**
     * Returns the current scheme ids, or throws if they are unset.
     *
     * @return the active scheme id
     * @throws IllegalStateException if the scheme id is unset
     */
    public long activeSchemeIdOrThrow() {
        if (schemeId == 0) {
            throw new IllegalStateException("No scheme id set");
        }
        return schemeId;
    }

    /**
     * Returns the active verification key, or throws if the context is not ready.
     *
     * @return the verification key
     */
    public Bytes verificationKeyOrThrow() {
        throwIfNotReady();
        return requireNonNull(construction)
                .hintsSchemeOrThrow()
                .preprocessedKeysOrThrow()
                .verificationKey();
    }

    /**
     * Returns the active construction ID, or throws if the context is not ready.
     *
     * @return the construction ID
     */
    public long constructionIdOrThrow() {
        throwIfNotReady();
        return requireNonNull(construction).constructionId();
    }

    /**
     * Validates a partial signature transaction body under the current hinTS construction.
     *
     * @param nodeId the node ID
     * @param crs the CRS to validate under
     * @param body the transaction body
     * @return true if the body is valid
     */
    public boolean validate(
            final long nodeId, @Nullable final Bytes crs, @NonNull final HintsPartialSignatureTransactionBody body) {
        if (crs == null || construction == null || nodePartyIds == null) {
            return false;
        }
        if (construction.constructionId() == body.constructionId() && nodePartyIds.containsKey(nodeId)) {
            final var preprocessedKeys = construction.hintsSchemeOrThrow().preprocessedKeysOrThrow();
            final var aggregationKey = preprocessedKeys.aggregationKey();
            final var partyId = nodePartyIds.get(nodeId);
            return library.verifyBls(crs, body.partialSignature(), body.message(), aggregationKey, partyId);
        }
        return false;
    }

    /**
     * Creates a new asynchronous signing process for the given block hash.
     *
     * @param blockHash the block hash
     * @param currentRoster the current roster
     * @return the signing process
     */
    public @NonNull Signing newSigning(
            @NonNull final Bytes blockHash,
            @NonNull final Roster currentRoster,
            @NonNull final Runnable onCompletion,
            final boolean aggregateImmediately) {
        requireNonNull(blockHash);
        throwIfNotReady();
        final var preprocessedKeys =
                requireNonNull(construction).hintsSchemeOrThrow().preprocessedKeysOrThrow();
        final var verificationKey = preprocessedKeys.verificationKey();
        final long totalWeight = currentRoster.rosterEntries().stream()
                .mapToLong(RosterEntry::weight)
                .sum();
        return new Signing(
                constructionIdOrThrow(),
                blockHash,
                requireNonNull(nodePartyIds),
                atLeastOneThirdOfTotal(totalWeight),
                crsOrThrow(),
                preprocessedKeys.aggregationKey(),
                verificationKey,
                currentRoster,
                onCompletion,
                aggregateImmediately);
    }

    /**
     * Returns the current CRS, or throws if it is missing.
     *
     * @throws NullPointerException if the CRS is missing
     */
    private @NonNull Bytes crsOrThrow() {
        return requireNonNull(crs);
    }

    /**
     * Returns the party assignments as a map of node IDs to party IDs.
     *
     * @param nodePartyIds the party assignments
     * @return the map of node IDs to party IDs
     */
    private static Map<Long, Integer> asNodePartyIds(@NonNull final List<NodePartyId> nodePartyIds) {
        return nodePartyIds.stream().collect(toMap(NodePartyId::nodeId, NodePartyId::partyId));
    }

    /**
     * Throws an exception if the context is not ready.
     */
    private void throwIfNotReady() {
        if (!isReady()) {
            throw new IllegalStateException("Signing context not ready");
        }
    }

    /**
     * A signing process spawned from this context.
     */
    public class Signing {
        private final long constructionId;
        private final long thresholdWeight;
        private final boolean aggregateImmediately;
        private final Bytes crs;
        private final Bytes aggregationKey;
        private final Bytes verificationKey;
        private final Bytes blockHash;
        private final Map<Long, Long> weights;
        private final Map<Long, Integer> partyIds;
        private final CompletableFuture<Bytes> future = new CompletableFuture<>();
        private final ConcurrentMap<Integer, Bytes> signatures = new ConcurrentHashMap<>();
        private final AtomicBoolean completed = new AtomicBoolean();
        private final AtomicLong verifiedWeight = new AtomicLong();
        long consensusWeight = 0L;

        @Nullable
        private final List<Integer> consensusPartyOrder;

        long pendingBatchWeight = 0L;

        @Nullable
        private volatile List<PendingSignature> pendingBatchSignatures = new ArrayList<>();

        public Signing(
                final long constructionId,
                @NonNull final Bytes blockHash,
                @NonNull final Map<Long, Integer> partyIds,
                final long thresholdWeight,
                @NonNull final Bytes crs,
                @NonNull final Bytes aggregationKey,
                @NonNull final Bytes verificationKey,
                @NonNull final Roster currentRoster,
                @NonNull final Runnable onCompletion,
                boolean aggregateImmediately) {
            this.thresholdWeight = thresholdWeight;
            this.constructionId = constructionId;
            this.aggregateImmediately = aggregateImmediately;
            requireNonNull(onCompletion);
            this.crs = requireNonNull(crs);
            this.aggregationKey = requireNonNull(aggregationKey);
            this.partyIds = requireNonNull(partyIds);
            this.verificationKey = requireNonNull(verificationKey);
            this.weights = currentRoster.rosterEntries().stream()
                    .collect(Collectors.toMap(RosterEntry::nodeId, RosterEntry::weight));
            this.blockHash = requireNonNull(blockHash);
            this.consensusPartyOrder = aggregateImmediately ? null : new ArrayList<>();
            scheduledExecutor.schedule(
                    () -> {
                        if (!future.isDone()) {
                            final var nodePartyIds = partyIds.entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                            log.warn(
                                    "Completing signing attempt on '{}' without obtaining a signature (had {} from parties {} for total weight {}/{} required)",
                                    blockHash,
                                    signatures.size(),
                                    signatures.keySet(),
                                    signatures.keySet().stream()
                                            .mapToLong(partyId -> weights.get(nodePartyIds.get(partyId)))
                                            .sum(),
                                    thresholdWeight);
                        }
                        onCompletion.run();
                    },
                    SIGNING_ATTEMPT_TIMEOUT.getSeconds(),
                    SECONDS);
        }

        /**
         * The future that will complete when sufficient partial signatures have been aggregated.
         *
         * @return the future
         */
        public CompletableFuture<Bytes> future() {
            return future;
        }

        public void trackOrder(final long nodeId, long constructionId) {
            requireNonNull(consensusPartyOrder);
            if (this.constructionId != constructionId || !partyIds.containsKey(nodeId) || completed.get()) {
                return;
            }
            consensusPartyOrder.add(partyIds.get(nodeId));
            consensusWeight += weights.getOrDefault(nodeId, 0L);
            if (consensusWeight >= thresholdWeight) {
                completed.set(true);
                final Map<Integer, Bytes> consensusSignatures = new HashMap<>();
                consensusPartyOrder.forEach(partyId -> {
                    final var sig = signatures.get(partyId);
                    if (sig != null) {
                        consensusSignatures.put(partyId, sig);
                    }
                });
                if (consensusSignatures.size() < consensusPartyOrder.size()) {
                    log.error(
                            "Known signatures from parties {} are fewer than {}",
                            consensusSignatures.keySet(),
                            consensusPartyOrder);
                } else {
                    CompletableFuture.runAsync(
                            () -> {
                                final var aggregatedSignature =
                                        library.aggregateSignatures(crs, aggregationKey, verificationKey, signatures);
                                future.complete(aggregatedSignature);
                            },
                            executor);
                }
            }
        }

        public void incorporate(final long nodeId, @NonNull final HintsPartialSignatureTransactionBody op) {
            requireNonNull(op);
            if (completed.get()) {
                return;
            }
            if (op.constructionId() == constructionId && partyIds.containsKey(nodeId)) {
                final long weight = weights.getOrDefault(nodeId, 0L);
                if (nodeId == selfNodeInfoSupplier.get().nodeId()) {
                    signatures.put(partyIds.get(nodeId), op.partialSignature());
                    verifiedWeight.addAndGet(weight);
                } else {
                    boolean inBatch = false;
                    if (pendingBatchSignatures != null) {
                        synchronized (this) {
                            if (pendingBatchSignatures != null) {
                                inBatch = true;
                                pendingBatchWeight += weight;
                                if (pendingBatchWeight >= thresholdWeight) {
                                    verifyBatchWith(nodeId, op.partialSignature());
                                    verifiedWeight.addAndGet(pendingBatchWeight);
                                } else {
                                    requireNonNull(pendingBatchSignatures)
                                            .add(new PendingSignature(nodeId, op.partialSignature()));
                                }
                            }
                        }
                    }
                    if (!inBatch) {
                        if (validate(nodeId, crs, op)) {
                            signatures.put(partyIds.get(nodeId), op.partialSignature());
                            verifiedWeight.addAndGet(weight);
                        }
                    }
                }
                if (aggregateImmediately && verifiedWeight.get() >= thresholdWeight) {
                    if (completed.compareAndSet(false, true)) {
                        final var aggregatedSignature =
                                library.aggregateSignatures(crs, aggregationKey, verificationKey, signatures);
                        future.complete(aggregatedSignature);
                    }
                }
            }
        }

        private void verifyBatchWith(final long nodeId, @NonNull final Bytes signature) {
            final Map<Integer, Bytes> partySigs = new HashMap<>();
            partySigs.put(partyIds.get(nodeId), signature);
            requireNonNull(pendingBatchSignatures)
                    .forEach(sig -> partySigs.put(partyIds.get(sig.nodeId()), sig.signature()));
            pendingBatchSignatures = null;
            if (library.verifyBlsBatch(crs, blockHash, aggregationKey, partySigs)) {
                signatures.putAll(partySigs);
            } else {
                // Should not reach here in test env
                throw new AssertionError("Not implemented");
            }
        }

        private record PendingSignature(long nodeId, @NonNull Bytes signature) {}
    }
}
