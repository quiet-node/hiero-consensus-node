package org.hiero.telemetryconverter;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneId;

public class Utils {
    static final String OPEN_TELEMETRY_SCHEMA_URL = "https://opentelemetry.io/schemas/1.0.0";
    private static final ZoneId UTC = ZoneId.of("UTC");

    static Bytes longToHashBytes(MessageDigest digest, long value) {
        // Convert the long value to a byte array and update the digest
        return Bytes.wrap(digest.digest(longToByteArray(value)));
    }

    static byte[] longToByteArray(long value) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (value >>> (56 - i * 8));
        }
        return bytes;
    }

    static Bytes longToBytes(long value) {
        return Bytes.wrap(longToByteArray(value));
    }

    /**
     * Converts an Instant to a UNIX Epoch time in nanoseconds.
     * TODO do we need to convert to UTC?
     *
     * @param instant the Instant to convert
     * @return the UNIX Epoch time in nanoseconds
     */
    static long instantToUnixEpocNanos(Instant instant) {
        // Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
        return instant.atZone(UTC).toEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    static long fileCreationTimeEpocNanos(Path filePath) throws IOException {
        try {
            BasicFileAttributes attributes = Files.readAttributes(filePath, BasicFileAttributes.class);
            final FileTime fileTime = attributes.creationTime();
            return instantToUnixEpocNanos(fileTime.toInstant());
        } catch (IOException e) {
            System.err.println("Error reading file attributes: " + e.getMessage());
            throw new UncheckedIOException(e);
        }
    }
}
