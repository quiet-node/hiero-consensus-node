// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.turtle.metric;

import com.swirlds.base.time.Time;
import com.swirlds.common.metrics.platform.SnapshotEvent;
import com.swirlds.metrics.api.Metric.DataType;
import com.swirlds.metrics.api.snapshot.Snapshot;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.consensus.model.node.NodeId;

/**
 * A collector that stores snapshot values from metrics, organized by NodeId,
 * metric identifier, and ValueType.
 */
public class MetricsCollector {

    private final Time time;
    private final Map<Identifier, List<NumberValue>> numericData = new ConcurrentHashMap<>();
    private final Map<Identifier, List<StringValue>> stringData = new ConcurrentHashMap<>();
    private final Map<Identifier, List<BooleanValue>> booleanData = new ConcurrentHashMap<>();

    public MetricsCollector(final Time time) {
        this.time = time;
    }

    /**
     * Handle a snapshot event and store all associated metric values in memory.
     *
     * @param snapshotEvent the event containing snapshot data per metric
     */
    public void handleSnapshots(final SnapshotEvent snapshotEvent) {
        final NodeId nodeId = snapshotEvent.nodeId();
        final Collection<Snapshot> snapshots = snapshotEvent.snapshots();

        for (final Snapshot snapshot : snapshots) {
            final String metricId = snapshot.metric().getIdentifier();
            final DataType dataType = snapshot.metric().getDataType();
            final Object value = snapshot.getValue();
            final Identifier key = new Identifier(nodeId, metricId);

            switch (dataType) {
                case INT, FLOAT -> {
                    if (value instanceof Number number) {
                        numericData
                                .computeIfAbsent(key, k -> new ArrayList<>())
                                .add(new NumberValue(time.now(), number));
                    }
                }
                case STRING -> {
                    if (value instanceof String str) {
                        stringData.computeIfAbsent(key, k -> new ArrayList<>()).add(new StringValue(time.now(), str));
                    }
                }
                case BOOLEAN -> {
                    if (value instanceof Boolean bool) {
                        booleanData
                                .computeIfAbsent(key, k -> new ArrayList<>())
                                .add(new BooleanValue(time.now(), bool));
                    }
                }
            }
        }
    }

    public List<NumberValue> getNumbers(@NonNull final NodeId nodeId, @NonNull final String metricId) {
        return numericData.get(new Identifier(nodeId, metricId));
    }

    record Identifier(NodeId nodeId, String metricId) {}

    public record NumberValue(Instant instant, Number value) {}

    public record StringValue(Instant instant, String value) {}

    public record BooleanValue(Instant instant, Boolean value) {}
}
