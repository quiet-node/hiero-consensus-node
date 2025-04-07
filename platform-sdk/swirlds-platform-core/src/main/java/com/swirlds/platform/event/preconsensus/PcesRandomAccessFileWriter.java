// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import com.hedera.hapi.platform.event.GossipEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;

/**
 * Writes preconsensus events to a file using a {@link RandomAccessFile}.
 */
public class PcesRandomAccessFileWriter implements PcesFileWriter {
    /** The file for writing events */
    private final RandomAccessFile file;

    /**
     * Create a new writer that writes events to a file using a {@link RandomAccessFile}.
     *
     * @param filePath       the path to the file to write to
     * @param syncEveryEvent if true, the file will be synced after every event is written
     * @throws IOException if an error occurs while opening the file
     */
    public PcesRandomAccessFileWriter(@NonNull final Path filePath, final boolean syncEveryEvent) throws IOException {
        file = new RandomAccessFile(filePath.toFile(), "rw");
    }

    @Override
    public void writeVersion(final int version) throws IOException {
        file.writeInt(version);
    }

    @Override
    public long writeEvent(@NonNull final GossipEvent event) throws IOException {
        final int value = GossipEvent.PROTOBUF.measureRecord(event);
        final var bytes = GossipEvent.PROTOBUF.toBytes(event);
        file.seek(file.length()); // Move to end for appending
        file.writeInt(value);
        file.write(bytes.toByteArray());
        return value;
    }

    @Override
    public void flush() throws IOException {
        // nothing to do here
    }

    @Override
    public void sync() throws IOException {
        file.getFD().sync();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    @Override
    public long fileSize() {
        try {
            return file.length();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
