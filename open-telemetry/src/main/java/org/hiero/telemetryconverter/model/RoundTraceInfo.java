package org.hiero.telemetryconverter.model;

import java.time.Instant;
import java.time.ZonedDateTime;

public record RoundTraceInfo(long roundNum,
                             EventType eventType,
                             long startTimeNanos,
                             long endTimeNanos) {

    public enum EventType {
        CREATED,
        EXECUTED
    }
}
