// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.metric;

// SPDX-License-Identifier: Apache-2.0

import com.swirlds.metrics.api.Metric;
import com.swirlds.metrics.api.MetricConfig;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import org.hiero.consensus.model.node.NodeId;

/**
 * In-memory implementation of the {@link Metrics} interface that is tailored for usage in otter
 * integration tests. It records the full history of every metric value so that assertions can be
 * executed after a test run.
 */
public class NodeMetrics implements Metrics {

    /** The node this metrics instance belongs to */
    private final NodeId nodeId;

    /** Map<identifier, metric> */
    private final Map<String, Metric> metrics = new ConcurrentHashMap<>();

    /** Map<identifier, history> */
    private final Map<String, List<Object>> history = new ConcurrentHashMap<>();

    /** Updaters that should be executed – executed on {@link #start()} in the test JVM */
    private final Set<Runnable> updaters = new CopyOnWriteArraySet<>();

    /** Flag indicating whether start() has been invoked */
    private volatile boolean started = false;

    public NodeMetrics(@NonNull final NodeId nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        // auto-register to the central singleton
        NetworkMetrics.getInstance().register(this);
    }

    /**
     * Returns the node id.
     *
     * @return the id
     */
    @NonNull
    public NodeId getNodeId() {
        return nodeId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        if (started) {
            return;
        }
        started = true;
        final Thread updaterThread = new Thread(
                () -> {
                    while (started) {
                        final Instant start = Instant.now();
                        updaters.forEach(Runnable::run);
                        final long sleepMillis = 100 - (Instant.now().toEpochMilli() - start.toEpochMilli());
                        if (sleepMillis > 0) {
                            try {
                                Thread.sleep(sleepMillis);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                },
                "NodeMetrics-updater-" + nodeId);
        updaterThread.setDaemon(true);
        updaterThread.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Metric getMetric(@NonNull final String category, @NonNull final String name) {
        Objects.requireNonNull(category, "category must not be null");
        Objects.requireNonNull(name, "name must not be null");
        return metrics.get(identifier(category, name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public Collection<Metric> findMetricsByCategory(@NonNull final String category) {
        Objects.requireNonNull(category, "category must not be null");
        return metrics.values().stream()
                .filter(m -> m.getCategory().startsWith(category))
                .toList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public Collection<Metric> getAll() {
        return Collections.unmodifiableCollection(metrics.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <T extends Metric> T getOrCreate(@NonNull final MetricConfig<T, ?> config) {
        Objects.requireNonNull(config, "config must not be null");
        final String id = identifier(config.getCategory(), config.getName());

        final Metric existing = metrics.get(id);
        if (existing != null) {
            // verify type compatibility
            if (!config.getResultClass().isAssignableFrom(existing.getClass())) {
                throw new IllegalStateException("Metric with category '" + config.getCategory() + "' and name '"
                        + config.getName() + "' already exists with incompatible type: " + existing.getClass());
            }
            return (T) existing;
        }

        // create dynamic proxy implementing the requested metric interface
        final Class<T> resultClass = config.getResultClass();
        final List<Object> valueHistory = new CopyOnWriteArrayList<>();
        history.put(id, valueHistory);

        final InvocationHandler handler = new TestMetricInvocationHandler(config, valueHistory);
        // Some metric interfaces extend others (e.g. SpeedometerMetric extends Metric) – we implement the main one
        final ClassLoader loader = resultClass.getClassLoader();
        final Class<?>[] interfaces;
        if (Metric.class.equals(resultClass)) {
            interfaces = new Class<?>[] {Metric.class};
        } else if (resultClass.isInterface()) {
            interfaces = new Class<?>[] {resultClass};
        } else {
            throw new IllegalStateException("Result class '" + resultClass + "' is not a Metric");
        }

        final T proxyInstance = (T) Proxy.newProxyInstance(loader, interfaces, handler);
        metrics.put(id, proxyInstance);

        return proxyInstance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(@NonNull final String category, @NonNull final String name) {
        Objects.requireNonNull(category, "category must not be null");
        Objects.requireNonNull(name, "name must not be null");
        final String id = identifier(category, name);
        metrics.remove(id);
        history.remove(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(@NonNull final Metric metric) {
        Objects.requireNonNull(metric, "metric must not be null");
        remove(metric.getCategory(), metric.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(@NonNull final MetricConfig<?, ?> config) {
        Objects.requireNonNull(config, "config must not be null");
        remove(config.getCategory(), config.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addUpdater(@NonNull final Runnable updater) {
        Objects.requireNonNull(updater, "updater must not be null");
        updaters.add(updater);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeUpdater(@NonNull final Runnable updater) {
        Objects.requireNonNull(updater, "updater must not be null");
        updaters.remove(updater);
    }

    /**
     * Get the full history of a metric.
     *
     * @param category metric category
     * @param name metric name
     * @return immutable list of recorded values (maybe empty) - no null values
     */
    @NonNull
    public List<Object> getHistory(@NonNull final String category, @NonNull final String name) {
        Objects.requireNonNull(category, "category must not be null");
        Objects.requireNonNull(name, "name must not be null");
        final List<Object> list = history.get(identifier(category, name));
        return list == null ? List.of() : list.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    private static String identifier(final String category, final String name) {
        return category + "." + name;
    }
}
