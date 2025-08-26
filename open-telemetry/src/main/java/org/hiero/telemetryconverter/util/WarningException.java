package org.hiero.telemetryconverter.util;

/**
 * Exception to indicate a warning condition that should be logged but processing should continue.
 */
public class WarningException extends RuntimeException {
    public WarningException(final String message) {
        super(message);
    }
}
