// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.metric;

import static com.swirlds.metrics.api.Metric.ValueType.VALUE;

import com.swirlds.metrics.api.Metric;
import com.swirlds.metrics.api.MetricConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Invocation handler that provides a minimal in-memory implementation for all metric interfaces. For all metric
 * specific write operations (e.g. {@code add()}, {@code update()}, {@code set()} ...) the current value is updated and
 * recorded. Read operations like {@code get()} return the last recorded value.
 */
final class TestMetricInvocationHandler implements InvocationHandler {

    private final MetricConfig<?, ?> config;
    private final List<Object> valueHistory;
    private volatile Object currentValue;

    TestMetricInvocationHandler(@NonNull final MetricConfig<?, ?> config, @NonNull final List<Object> valueHistory) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.valueHistory = Objects.requireNonNull(valueHistory, "valueHistory cannot be null");
        // initialize current value
        this.currentValue = numericZero(configDataType());
    }

    @Override
    @Nullable
    public Object invoke(@NonNull final Object proxy, @NonNull final Method method, @Nullable final Object[] args)
            throws Throwable {
        Objects.requireNonNull(proxy, "proxy cannot be null");
        Objects.requireNonNull(method, "method cannot be null");
        final String name = method.getName();

        // Generic Metric interface methods ------------------------------------------------
        switch (name) {
            case "getCategory" -> {
                return config.getCategory();
            }
            case "getName" -> {
                return config.getName();
            }
            case "getDescription" -> {
                return config.getDescription();
            }
            case "getUnit" -> {
                return config.getUnit();
            }
            case "getFormat" -> {
                return config.getFormat();
            }
            case "getMetricType" -> {
                return null; // not accessible form here
            }
            case "getDataType" -> {
                return configDataType();
            }
            case "getValueTypes" -> {
                return EnumSet.of(VALUE);
            }
            case "get" -> {
                if (args == null || args.length == 0) {
                    // get() method without parameters
                    return currentValue;
                }
                // get(ValueType)
                return currentValue;
            }
            case "reset" -> {
                currentValue = numericZero(configDataType());
                record(currentValue);
                return null;
            }
            case "equals" -> {
                Objects.requireNonNull(args, "args cannot be null");
                final Object other = args[0];
                if (other == null) {
                    return false;
                }
                if (!(other instanceof Metric m)) {
                    return false;
                }
                return m.getClass().equals(proxy.getClass())
                        && m.getCategory().equals(config.getCategory())
                        && m.getName().equals(config.getName());
            }
            case "hashCode" -> {
                return Objects.hash(proxy.getClass(), config.getCategory(), config.getName());
            }
            default -> {
                // continue to more specific handling
            }
        }

        // Metric specific write operations ------------------------------------------------
        if (name.equals("increment") && (args == null || args.length == 0)) {
            synchronized (this) {
                currentValue = ((Number) currentValue).longValue() + 1L;
                record(currentValue);
            }
            return null;
        }
        if (name.equals("add") && args != null && args.length == 1) {
            synchronized (this) {
                final long delta = ((Number) args[0]).longValue();
                currentValue = ((Number) currentValue).longValue() + delta;
                record(currentValue);
            }
            return null;
        }
        if (name.equals("update") && args != null && args.length == 1) {
            currentValue = args[0];
            record(currentValue);
            return null;
        }
        if (name.equals("update") && args != null && args.length == 2) {
            // IntegerPairAccumulator update(left,right) – we just record right now the resultFunction output if any
            currentValue = List.of(args[0], args[1]);
            record(currentValue);
            return null;
        }
        if (name.equals("set") && args != null && args.length == 1) {
            currentValue = args[0];
            record(currentValue);
            return null;
        }
        if (name.equals("cycle") && (args == null || args.length == 0)) {
            synchronized (this) {
                // just record cycle occurrence by incrementing internal count
                currentValue = ((Number) currentValue).longValue() + 1L;
                record(currentValue);
            }
            return null;
        }
        if (name.equals("getNanos") && (args == null || args.length == 0)) {
            return currentValue instanceof Number ? ((Number) currentValue).longValue() : 0L;
        }

        throw new IllegalArgumentException("Unknown method: " + name);
    }

    private Metric.DataType configDataType() {
        try {
            return MetricConfig.mapDataType(config.getResultClass());
        } catch (final Exception e) {
            return Metric.DataType.FLOAT;
        }
    }

    private void record(final Object value) {
        valueHistory.add(value);
    }

    private static Object numericZero(final Metric.DataType dataType) {
        return switch (dataType) {
            case FLOAT -> 0.0;
            case INT -> 0L;
            default -> 0;
        };
    }
}
