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
import org.hiero.otter.fixtures.result.MultipleNodeReconnectResults;
import org.hiero.otter.fixtures.result.SingleNodeReconnectResult;

/**
 * Continuous assertions for {@link MultipleNodeReconnectResults}.
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public class MultipleNodeReconnectResultContinuousAssert
        extends AbstractMultipleNodeContinuousAssertion<
                MultipleNodeReconnectResultContinuousAssert, MultipleNodeReconnectResults> {

    /**
     * Creates a continuous assertion for the given {@link MultipleNodeReconnectResults}.
     *
     * @param actual the actual {@link MultipleNodeReconnectResults} to assert
     */
    public MultipleNodeReconnectResultContinuousAssert(@Nullable final MultipleNodeReconnectResults actual) {
        super(actual, MultipleNodeReconnectResultContinuousAssert.class);
    }

    /**
     * Creates a continuous assertion for the given {@link MultipleNodeReconnectResults}.
     *
     * @param actual the {@link MultipleNodeReconnectResults} to assert
     * @return a continuous assertion for the given {@link MultipleNodeReconnectResults}
     */
    @NonNull
    public static MultipleNodeReconnectResultContinuousAssert assertContinuouslyThat(
            @Nullable final MultipleNodeReconnectResults actual) {
        return new MultipleNodeReconnectResultContinuousAssert(actual);
    }

    /**
     * Asserts that the node has no failed reconnects.
     *
     * @return a continuous assertion for the given {@link SingleNodeReconnectResult}
     */
    @NonNull
    public MultipleNodeReconnectResultContinuousAssert hasNoFailedReconnects() {
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
    public MultipleNodeReconnectResultContinuousAssert doNotAttemptToReconnect() {
        return checkContinuously((logEntry) -> {
            if (logEntry.message().contains(ReconnectStartPayload.class.toString())) {
                failWithMessage("Expected no failed reconnect, but found %s", logEntry.message());
            }
        });
    }

    private MultipleNodeReconnectResultContinuousAssert checkContinuously(final Consumer<StructuredLog> check) {
        isNotNull();

        final LogSubscriber subscriber = logEntry -> switch (state) {
            case ACTIVE -> {
                if (!suppressedNodeIds.contains(logEntry.nodeId())) {
                    check.accept(logEntry);
                }
                yield CONTINUE;
            }
            case PAUSED -> CONTINUE;
            case DESTROYED -> UNSUBSCRIBE;
        };

        actual.subscribe(subscriber);

        return this;
    }
}
