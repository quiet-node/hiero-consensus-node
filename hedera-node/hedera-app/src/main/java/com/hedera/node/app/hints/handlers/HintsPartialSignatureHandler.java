// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.hints.handlers;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.services.auxiliary.hints.HintsPartialSignatureTransactionBody;
import com.hedera.node.app.hints.ReadableHintsStore;
import com.hedera.node.app.hints.impl.HintsContext;
import com.hedera.node.app.spi.workflows.HandleContext;
import com.hedera.node.app.spi.workflows.HandleException;
import com.hedera.node.app.spi.workflows.PreCheckException;
import com.hedera.node.app.spi.workflows.PreHandleContext;
import com.hedera.node.app.spi.workflows.PureChecksContext;
import com.hedera.node.app.spi.workflows.TransactionHandler;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class HintsPartialSignatureHandler implements TransactionHandler {
    private static final Logger log = LogManager.getLogger(HintsContext.class);

    @NonNull
    private final ConcurrentMap<Bytes, HintsContext.Signing> signings;

    private final AtomicReference<Roster> currentRoster;

    private final HintsContext hintsContext;

    private final LoadingCache<PartialSignature, Validation> cache;

    /**
     * A node's partial signature verified relative to a particular hinTS construction id and CRS.
     *
     * @param constructionId the construction id
     * @param crs the CRS
     * @param nodeId the node id
     * @param body the partial signature
     */
    private record PartialSignature(
            long constructionId, @NonNull Bytes crs, long nodeId, @NonNull HintsPartialSignatureTransactionBody body) {
        private PartialSignature {
            requireNonNull(crs);
            requireNonNull(body);
        }
    }

    /**
     * The result of validating a signature and computing an optimistic aggregated signature if the signature
     * is valid and likely to trigger the final aggregation process.
     * @param valid whether the signature is valid
     * @param result if applicable, the result of aggregation assuming this was the last valid signature needed
     */
    private record Validation(boolean valid, @Nullable HintsContext.Signing.Result result) {}

    @Inject
    public HintsPartialSignatureHandler(
            @NonNull final Duration blockPeriod,
            @NonNull final ConcurrentMap<Bytes, HintsContext.Signing> signings,
            @NonNull final HintsContext context,
            @NonNull final AtomicReference<Roster> currentRoster) {
        this.signings = requireNonNull(signings);
        this.hintsContext = requireNonNull(context);
        this.currentRoster = requireNonNull(currentRoster);
        cache = Caffeine.newBuilder()
                .expireAfterAccess(Math.max(1, 2 * blockPeriod.getSeconds()), TimeUnit.SECONDS)
                .softValues()
                .build(this::computeValidation);
    }

    @Override
    public void pureChecks(@NonNull final PureChecksContext context) throws PreCheckException {
        requireNonNull(context);
    }

    @Override
    public void preHandle(@NonNull final PreHandleContext context) throws PreCheckException {
        requireNonNull(context);
        final var op = context.body().hintsPartialSignatureOrThrow();
        final var hintsStore = context.createStore(ReadableHintsStore.class);
        // We don't care about the result, just that it's in the cache
        cache.get(new PartialSignature(
                hintsContext.constructionIdOrThrow(),
                requireNonNull(hintsStore.crsIfKnown()),
                context.creatorInfo().nodeId(),
                op));
    }

    @Override
    public void handle(@NonNull final HandleContext context) throws HandleException {
        requireNonNull(context);
        final var op = context.body().hintsPartialSignatureOrThrow();
        final var creator = context.creatorInfo().nodeId();
        final var hintsStore = context.storeFactory().readableStore(ReadableHintsStore.class);
        final var crs = requireNonNull(hintsStore.crsIfKnown());
        final var signature = new PartialSignature(
                hintsContext.constructionIdOrThrow(), crs, context.creatorInfo().nodeId(), op);
        final var validation = requireNonNull(cache.get(signature));
        if (validation.valid()) {
            final var signing = signings.computeIfAbsent(
                    op.message(),
                    b -> hintsContext.newSigning(
                            b, requireNonNull(currentRoster.get()), () -> signings.remove(op.message())));
            signing.incorporateValid(crs, creator, op.partialSignature(), validation.result());
        }
    }

    /**
     * Validates the given partial signature.
     *
     * @param signature the partial signature to validate
     * @return whether the partial signature is valid
     */
    private @Nullable Validation computeValidation(@NonNull final PartialSignature signature) {
        try {
            final var op = signature.body();
            final var crs = signature.crs();
            final long nodeId = signature.nodeId();
            final boolean valid = hintsContext.validate(nodeId, crs, op);
            HintsContext.Signing.Result result = null;
            if (valid) {
                final var signing = signings.computeIfAbsent(
                        op.message(),
                        b -> hintsContext.newSigning(
                                b, requireNonNull(currentRoster.get()), () -> signings.remove(op.message())));
                result = signing.resultIfLikelyToTriggerAggregation(nodeId, op.partialSignature(), crs);
            }
            return new Validation(valid, result);
        } catch (ConcurrentModificationException ignore) {
            // It's technically _possible_ this could throw some form of CME during pre-handle, so
            // just return null in that case (i.e., don't cache the result); and then revalidate
            // synchronously during handle if this ever happens.
            return null;
        } catch (Exception e) {
            log.error("Unexpected error validating partial signature {}", signature, e);
            return null;
        }
    }
}
