// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import com.swirlds.platform.event.preconsensus.AbstractPcesFileChannelWriter.DsyncPcesFileChannelWriter;
import com.swirlds.platform.event.preconsensus.AbstractPcesFileChannelWriter.PcesFileChannelWriter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;

public enum PcesFileWriterType {
    OUTPUT_STREAM,
    FILE_CHANNEL_DSYNC,
    FILE_CHANNEL,
    RANDOM_ACCESS_FILE;

    @NonNull
    public PcesFileWriter create(@NonNull final Path filePath) throws IOException {
        return switch (this) {
            case FILE_CHANNEL_DSYNC -> new DsyncPcesFileChannelWriter(filePath);
            case FILE_CHANNEL -> new PcesFileChannelWriter(filePath);
            case RANDOM_ACCESS_FILE -> new PcesRandomAccessFileWriter(filePath);
            default -> new PcesOutputStreamFileWriter(filePath);
        };
    }
}
