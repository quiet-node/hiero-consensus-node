package org.hiero.telemetryconverter.model.trace;

public record BlockTraceInfo(long nodeId,
                             long blockNumber,// EventCore.hashCode() value
                             EventType eventType,
                             long startTimeNanos,
                             long endTimeNanos) {
    public enum EventType {
        CREATED,
    }
}
