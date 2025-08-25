package org.hiero.telemetryconverter.model.trace;

public record EventTraceInfo(long nodeId,
                             int eventHash,// EventCore.hashCode() value
                             EventType eventType,
                             long startTimeNanos,
                             long endTimeNanos) {
    public enum EventType {
        CREATED,
        GOSSIPED,
        RECEIVED,
        PRE_HANDLED,
    }
}
