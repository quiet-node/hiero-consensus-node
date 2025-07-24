// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.internal.result;

import com.hedera.hapi.platform.state.NodeId;
import com.swirlds.logging.legacy.payload.LogPayload;
import com.swirlds.logging.legacy.payload.ReconnectFailurePayload;
import com.swirlds.logging.legacy.payload.ReconnectFinishPayload;
import com.swirlds.logging.legacy.payload.SynchronizationCompletePayload;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.hiero.otter.fixtures.assertions.SingleNodeLogResultContinuousAssert;
import org.hiero.otter.fixtures.logging.StructuredLog;
import org.hiero.otter.fixtures.result.LogSubscriber;
import org.hiero.otter.fixtures.result.ReconnectSubscriber;
import org.hiero.otter.fixtures.result.SingleNodeLogResult;
import org.hiero.otter.fixtures.result.SingleNodeReconnectResult;
import org.hiero.otter.fixtures.result.SubscriberAction;
import org.hyperledger.besu.evm.log.Log;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Fail.fail;
import static org.hiero.otter.fixtures.internal.helpers.LogPayloadUtils.parsePayload;
import static org.hiero.otter.fixtures.result.SubscriberAction.CONTINUE;
import static org.hiero.otter.fixtures.result.SubscriberAction.UNSUBSCRIBE;

/**
 * Implementation of the {@link SingleNodeReconnectResult} interface.
 */
public class SingleNodeReconnectResultImpl implements SingleNodeReconnectResult {

    private final NodeId nodeId;
    private final SingleNodeLogResult logResults;
    private final Set<Class<? extends LogPayload>> reconnectPayloadClasses;
    private final Map<Class<? extends LogPayload>, List<ReconnectSubscriber<? extends LogPayload>>> subscribers;

    /**
     * Constructor for SingleNodeReconnectResultImpl.
     *
     * @param logResults the log results for the single node
     */
    public SingleNodeReconnectResultImpl(
            @NonNull final NodeId nodeId,
            @NonNull final SingleNodeLogResult logResults) {
        this.nodeId = requireNonNull(nodeId);
        this.logResults = requireNonNull(logResults);
        subscribers = new ConcurrentHashMap<>();
        reconnectPayloadClasses = Set.of(
                ReconnectFinishPayload.class,
                ReconnectFailurePayload.class,
                SynchronizationCompletePayload.class
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @NotNull com.hedera.hapi.platform.state.NodeId nodeId() {
        return nodeId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int numSuccessfulReconnects() {
        return (int) logResults.logs().stream()
                .filter(log -> log.message().contains(ReconnectFinishPayload.class.toString()))
                .count();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int numFailedReconnects() {
        return (int) logResults.logs().stream()
                .filter(log -> log.message().contains(ReconnectFailurePayload.class.toString()))
                .count();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends LogPayload> void subscribe(@NotNull final T clazz,
            @NotNull final ReconnectSubscriber<T> subscriber) {
        if (!reconnectPayloadClasses.contains(clazz.getClass())) {
            fail("Unsupported payload class: %s", clazz.getClass());
        }
        if (subscribers.isEmpty()) {
            logResults.subscribe(this::onLogEntry);
        }
        subscribers.getOrDefault(clazz.getClass(), new ArrayList<>()).add(subscriber);
    }

    private SubscriberAction onLogEntry(@NonNull final StructuredLog logEntry) {
        final String message = logEntry.message();
        reconnectPayloadClasses.forEach(clazz -> {
            if (message.contains(clazz.toString())) {
                final List<ReconnectSubscriber<? extends LogPayload>> subscriberList =
                        subscribers.getOrDefault(clazz, new ArrayList<>());
                if (subscriberList.isEmpty()) {
                    return;
                }
                final LogPayload payload = parsePayload(clazz, message);
                for (ReconnectSubscriber<? extends LogPayload> subscriber : subscriberList) {
                    final SubscriberAction action = subscriber.onPayload(clazz.cast(payload));
                    if (action == UNSUBSCRIBE) {
                        subscriberList.remove(subscriber);
                    }
                }
            }
        });
        if (subscribers.isEmpty()) {
            return UNSUBSCRIBE;
        } else {
            return CONTINUE;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public List<SynchronizationCompletePayload> getSynchronizationCompletePayloads() {
        return logResults.logs().stream()
                .filter(log -> log.message().contains(SynchronizationCompletePayload.class.toString()))
                .map(log -> parsePayload(SynchronizationCompletePayload.class, log.message()))
                .toList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        logResults.clear();
    }
}
