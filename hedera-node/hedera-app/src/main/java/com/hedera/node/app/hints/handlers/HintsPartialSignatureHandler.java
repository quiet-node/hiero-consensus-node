// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.hints.handlers;

import static com.hedera.node.config.types.SigAggregationPoint.CONSENSUS;
import static com.hedera.node.config.types.SigAggregationPoint.PREHANDLE;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.node.app.hints.impl.HintsContext;
import com.hedera.node.app.spi.workflows.HandleContext;
import com.hedera.node.app.spi.workflows.HandleException;
import com.hedera.node.app.spi.workflows.PreCheckException;
import com.hedera.node.app.spi.workflows.PreHandleContext;
import com.hedera.node.app.spi.workflows.PureChecksContext;
import com.hedera.node.app.spi.workflows.TransactionHandler;
import com.hedera.node.config.data.TssConfig;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class HintsPartialSignatureHandler implements TransactionHandler {
    private static final Logger log = LogManager.getLogger(HintsPartialSignatureHandler.class);

    @NonNull
    private final ConcurrentMap<Bytes, HintsContext.Signing> signings;

    private final AtomicReference<Roster> currentRoster;

    private final HintsContext hintsContext;

    @Inject
    public HintsPartialSignatureHandler(
            @NonNull final Duration blockPeriod,
            @NonNull final ConcurrentMap<Bytes, HintsContext.Signing> signings,
            @NonNull final HintsContext context,
            @NonNull final AtomicReference<Roster> currentRoster) {
        this.signings = requireNonNull(signings);
        this.hintsContext = requireNonNull(context);
        this.currentRoster = requireNonNull(currentRoster);
    }

    @Override
    public void pureChecks(@NonNull final PureChecksContext context) throws PreCheckException {
        requireNonNull(context);
    }

    @Override
    public void preHandle(@NonNull final PreHandleContext context) throws PreCheckException {
        requireNonNull(context);
        final var op = context.body().hintsPartialSignatureOrThrow();
        final var creatorId = context.creatorInfo().nodeId();
        final var aggregateImmediately =
                context.configuration().getConfigData(TssConfig.class).sigAggregationPoint() == PREHANDLE;
        signings.computeIfAbsent(
                        op.message(),
                        b -> hintsContext.newSigning(
                                b,
                                requireNonNull(currentRoster.get()),
                                () -> signings.remove(op.message()),
                                aggregateImmediately))
                .incorporate(creatorId, op);
    }

    @Override
    public void handle(@NonNull final HandleContext context) throws HandleException {
        requireNonNull(context);
        final var sigAggregationPoint =
                context.configuration().getConfigData(TssConfig.class).sigAggregationPoint();
        if (sigAggregationPoint == CONSENSUS) {
            final var op = context.body().hintsPartialSignatureOrThrow();
            final var creatorId = context.creatorInfo().nodeId();
            signings.computeIfAbsent(
                            op.message(),
                            b -> hintsContext.newSigning(
                                    b, requireNonNull(currentRoster.get()), () -> signings.remove(op.message()), false))
                    .trackOrder(creatorId, op.constructionId());
        }
    }
}
