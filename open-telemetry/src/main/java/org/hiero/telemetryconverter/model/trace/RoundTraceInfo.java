package org.hiero.telemetryconverter.model.trace;

public record RoundTraceInfo(long nodeId,
                             long roundNum,
                             EventType eventType,
                             long startTimeNanos,
                             long endTimeNanos) {

    public enum EventType {
        CREATED,
        EXECUTED,
        HASHED
    }
}
