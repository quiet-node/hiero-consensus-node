// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.assertions;

import static org.hiero.otter.fixtures.internal.helpers.LogPayloadUtils.parsePayload;
import static org.hiero.otter.fixtures.result.SubscriberAction.CONTINUE;
import static org.hiero.otter.fixtures.result.SubscriberAction.UNSUBSCRIBE;

import com.swirlds.logging.legacy.payload.ReconnectFailurePayload;
import com.swirlds.logging.legacy.payload.ReconnectStartPayload;
import com.swirlds.logging.legacy.payload.SynchronizationCompletePayload;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.function.Consumer;
import org.hiero.otter.fixtures.logging.StructuredLog;
import org.hiero.otter.fixtures.result.LogSubscriber;
import org.hiero.otter.fixtures.result.SingleNodePlatformStatusResults;
import org.hiero.otter.fixtures.result.SingleNodeReconnectResult;

/**
 * Continuous assertions for {@link SingleNodeReconnectResult}.
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public class SingleNodeReconnectResultContinuousAssert
        extends AbstractContinuousAssertion<SingleNodeReconnectResultContinuousAssert, SingleNodeReconnectResult> {

    /**
     * Creates a continuous assertion for the given {@link SingleNodePlatformStatusResults}.
     *
     * @param actual the actual {@link SingleNodePlatformStatusResults} to assert
     */
    public SingleNodeReconnectResultContinuousAssert(@Nullable final SingleNodeReconnectResult actual) {
        super(actual, SingleNodeReconnectResultContinuousAssert.class);
    }

    /**
     * Creates a continuous assertion for the given {@link SingleNodePlatformStatusResults}.
     *
     * @param actual the {@link SingleNodePlatformStatusResults} to assert
     * @return a continuous assertion for the given {@link SingleNodePlatformStatusResults}
     */
    @NonNull
    public static SingleNodeReconnectResultContinuousAssert assertContinuouslyThat(
            @Nullable final SingleNodeReconnectResult actual) {
        return new SingleNodeReconnectResultContinuousAssert(actual);
    }

    /**
     * Asserts that the node has no failed reconnects.
     *
     * @return a continuous assertion for the given {@link SingleNodeReconnectResult}
     */
    @NonNull
    public SingleNodeReconnectResultContinuousAssert hasNoFailedReconnects() {
        return checkContinuously(logEntry -> {
            if (logEntry.message().contains(ReconnectFailurePayload.class.toString())) {
                failWithMessage("Expected no failed reconnects, but found %s", logEntry.message());
            }
        });
    }

    /**
     * Asserts that the node does attempt to perform any reconnects.
     *
     * @return a continuous assertion for the given {@link SingleNodeReconnectResult}
     */
    @NonNull
    public SingleNodeReconnectResultContinuousAssert doesNotAttemptToReconnect() {
        return checkContinuously(logEntry -> {
            if (logEntry.message().contains(ReconnectStartPayload.class.toString())) {
                failWithMessage("Expected no failed reconnect, but found %s", logEntry.message());
            }
        });
    }

    /**
     * Asserts that the node has no reconnects that take longer than the provided time.
     *
     * @param maximumReconnectTime the maximum allowed reconnect time
     * @return a continuous assertion for the given {@link SingleNodeReconnectResultContinuousAssert}
     */
    @NonNull
    public SingleNodeReconnectResultContinuousAssert hasMaximumReconnectTime(
            @NonNull final Duration maximumReconnectTime) {
        return checkContinuously(logEntry -> {
            if (logEntry.message().contains(SynchronizationCompletePayload.class.toString())) {
                final SynchronizationCompletePayload payload =
                        parsePayload(SynchronizationCompletePayload.class, logEntry.message());
                if (payload.getTimeInSeconds() > maximumReconnectTime.getSeconds()) {
                    failWithMessage(
                            "Expected maximum reconnect time to be <%s> but found <%s>",
                            maximumReconnectTime, Duration.ofSeconds((long) payload.getTimeInSeconds()));
                }
            }
        });
    }

    /**
     * Asserts that the node has no reconnects that take longer than the provided time to initialize the tree.
     *
     * @param maximumTreeInitializationTime the maximum allowed tree initialization time
     * @return a continuous assertion for the given {@link SingleNodeReconnectResultContinuousAssert}
     */
    @NonNull
    public SingleNodeReconnectResultContinuousAssert hasMaximumTreeInitializationTime(
            final Duration maximumTreeInitializationTime) {
        return checkContinuously(logEntry -> {
            if (logEntry.message().contains(SynchronizationCompletePayload.class.toString())) {
                final SynchronizationCompletePayload payload =
                        parsePayload(SynchronizationCompletePayload.class, logEntry.message());
                if (payload.getInitializationTimeInSeconds() > maximumTreeInitializationTime.getSeconds()) {
                    failWithMessage(
                            "Expected maximum tree initialization time to be <%s> but found <%s>",
                            maximumTreeInitializationTime,
                            Duration.ofSeconds((long) payload.getInitializationTimeInSeconds()));
                }
            }
        });
    }

    @NonNull
    private SingleNodeReconnectResultContinuousAssert checkContinuously(@NonNull final Consumer<StructuredLog> check) {
        isNotNull();

        final LogSubscriber subscriber = logEntry -> switch (state) {
            case ACTIVE -> {
                check.accept(logEntry);
                yield CONTINUE;
            }
            case PAUSED -> CONTINUE;
            case DESTROYED -> UNSUBSCRIBE;
        };

        actual.subscribe(subscriber);

        return this;
    }
}
