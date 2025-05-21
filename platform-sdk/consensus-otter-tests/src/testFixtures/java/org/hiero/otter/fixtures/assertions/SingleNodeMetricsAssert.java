// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.assertions;

// SPDX-License-Identifier: Apache-2.0

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import org.assertj.core.api.AbstractAssert;
import org.hiero.otter.fixtures.result.SingleNodeMetricsResult;
import org.hiero.otter.fixtures.turtle.metric.MetricsCollector.NumberValue;

/**
 * Assertion class for {@link SingleNodeMetricsResult}.
 *
 * <p>Provides custom assertions for validating metric results for one node.
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
public class SingleNodeMetricsAssert extends AbstractAssert<SingleNodeMetricsAssert, SingleNodeMetricsResult> {

    /**
     * Constructs an assertion for the given {@link SingleNodeMetricsResult}.
     *
     * @param actual the actual {@link SingleNodeMetricsResult} to assert
     */
    protected SingleNodeMetricsAssert(@Nullable final SingleNodeMetricsResult actual) {
        super(actual, SingleNodeMetricsAssert.class);
    }

    /**
     * Creates an assertion for the given {@link SingleNodeMetricsResult}.
     *
     * @param actual the actual {@link SingleNodeMetricsResult} to assert
     * @return a new instance of {@link SingleNodeMetricsResult}
     */
    public static SingleNodeMetricsAssert assertThat(@Nullable final SingleNodeMetricsResult actual) {
        return new SingleNodeMetricsAssert(actual);
    }

    /**
     * Verifies that the specified metric value was at no point higher than the given threshold.
     *
     * @param threshold the exclusive upper bound
     * @return the {@link SingleNodeMetricsResult} instance
     */
    public SingleNodeMetricsAssert neverExceeds(final double threshold) {
        isNotNull();
        final boolean exceeded = actual.history().stream()
                .map(NumberValue::value)
                .mapToDouble(Number::doubleValue)
                .anyMatch(v -> v > threshold);
        if (exceeded) {
            final List<Double> wrongValues = actual.history().stream()
                    .map(NumberValue::value)
                    .map(Number::doubleValue)
                    .filter(v -> v > threshold)
                    .toList();
            failWithMessage(
                    "Metric %s exceeded threshold %.3f. Values: %s", actual.identifier(), threshold, wrongValues);
        }
        return this;
    }
}
