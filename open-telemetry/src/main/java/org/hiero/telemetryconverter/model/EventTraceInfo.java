package org.hiero.telemetryconverter.model;

import java.time.Instant;

public record EventTraceInfo(int eventHash,// EventCore.hashCode() value
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
