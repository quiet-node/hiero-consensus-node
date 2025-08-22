package org.hiero.telemetryconverter.model;

import java.time.Instant;

public record TransactionTraceInfo(int txHash,
                                   EventType eventType,
                                   long startTimeNanos,
                                   long endTimeNanos) {
    public enum EventType {
        RECEIVED,
        EXECUTED
    }
}
