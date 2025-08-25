package org.hiero.telemetryconverter.model.trace;

public record TransactionTraceInfo(long nodeId,
                                   int txHash, // TransactionID.hashCode() value
                                   EventType eventType,
                                   long startTimeNanos,
                                   long endTimeNanos) {
    public enum EventType {
        RECEIVED,
        EXECUTED
    }
}
