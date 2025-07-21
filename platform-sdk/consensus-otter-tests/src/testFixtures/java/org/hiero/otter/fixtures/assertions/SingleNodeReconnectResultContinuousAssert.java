// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.assertions;

import static org.hiero.otter.fixtures.result.SubscriberAction.CONTINUE;
import static org.hiero.otter.fixtures.result.SubscriberAction.UNSUBSCRIBE;

import com.swirlds.logging.legacy.payload.ReconnectFailurePayload;
import com.swirlds.logging.legacy.payload.ReconnectStartPayload;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
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
