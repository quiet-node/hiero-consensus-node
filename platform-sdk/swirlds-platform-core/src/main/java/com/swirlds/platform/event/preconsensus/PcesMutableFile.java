// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.hiero.consensus.model.event.PlatformEvent;

/**
 * Represents a preconsensus event file that can be written to.
 */
public class PcesMutableFile {
    /**
     * Describes the file that is being written to.
     */
    private final PcesFile descriptor;

    private final PcesFileWriter writer;

    /**
     * The highest ancient indicator of all events written to the file.
     */
    private long highestAncientIdentifierInFile;

    /**
     * Create a new preconsensus event file that can be written to.
     *
     * @param descriptor     a description of the file
     * @param pcesFileWriterType whether to use a FileChannel to write to the file as opposed to an OutputStream
     * @param syncEveryEvent whether to sync the file after every event
     */
    PcesMutableFile(
            @NonNull final PcesFile descriptor,
            final PcesFileWriterType pcesFileWriterType,
            final boolean syncEveryEvent)
            throws IOException {
        if (Files.exists(descriptor.getPath())) {
            throw new IOException("File " + descriptor.getPath() + " already exists");
        }

        Files.createDirectories(descriptor.getPath().getParent());

        this.descriptor = descriptor;
        writer = pcesFileWriterType.create(descriptor.getPath(), syncEveryEvent);
        writer.writeVersion(PcesFileVersion.currentVersionNumber());
        highestAncientIdentifierInFile = descriptor.getLowerBound();
    }

    public enum PcesFileWriterType {
        OUTPUT_STREAM,
        FILE_CHANNEL,
        RANDOM_ACCESS_FILE;

        PcesFileWriter create(@NonNull final Path filePath, final boolean syncEveryEvent) throws IOException {
            return switch (this) {
                case FILE_CHANNEL -> new PcesFileChannelWriter(filePath, syncEveryEvent);
                case RANDOM_ACCESS_FILE -> new PcesRandomAccessFileWriter(filePath, syncEveryEvent);
                default -> new PcesOutputStreamFileWriter(filePath, syncEveryEvent);
            };
        }
    }
    /**
     * Check if this file is eligible to contain an event based on bounds.
     *
     * @param ancientIdentifier the ancient indicator of the event in question
     * @return true if this file is eligible to contain the event
     */
    public boolean canContain(final long ancientIdentifier) {
        return descriptor.canContain(ancientIdentifier);
    }

    /**
     * Write an event to the file.
     *
     * @param event the event to write
     */
    public long writeEvent(final PlatformEvent event) throws IOException {
        if (!descriptor.canContain(event.getAncientIndicator(descriptor.getFileType()))) {
            throw new IllegalStateException("Cannot write event " + event.getHash() + " with ancient indicator "
                    + event.getAncientIndicator(descriptor.getFileType()) + " to file " + descriptor);
        }
        final var value = writer.writeEvent(event.getGossipEvent());
        highestAncientIdentifierInFile =
                Math.max(highestAncientIdentifierInFile, event.getAncientIndicator(descriptor.getFileType()));
        return value;
    }

    /**
     * Atomically rename this file so that its un-utilized span is 0.
     *
     * @param upperBoundInPreviousFile the previous file's upper bound. Even if we are not utilizing the
     *                                        entire span of this file, we cannot reduce the upper bound so that
     *                                        it is smaller than the previous file's highest upper bound.
     * @return the new span compressed file
     */
    public PcesFile compressSpan(final long upperBoundInPreviousFile) {
        if (highestAncientIdentifierInFile == descriptor.getUpperBound()) {
            // No need to compress, we used the entire span.
            return descriptor;
        }

        final PcesFile newDescriptor = descriptor.buildFileWithCompressedSpan(
                Math.max(highestAncientIdentifierInFile, upperBoundInPreviousFile));

        try {
            Files.move(descriptor.getPath(), newDescriptor.getPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        return newDescriptor;
    }

    /**
     * Flush the file.
     */
    public void flush() throws IOException {
        writer.flush();
    }

    /**
     * Sync the buffer with the file system.
     */
    public void sync() throws IOException {
        writer.sync();
    }

    /**
     * Close the file.
     */
    public void close() throws IOException {
        writer.close();
    }

    /**
     * Get the current size of the file, in bytes.
     *
     * @return the size of the file in bytes
     */
    public long fileSize() {
        return writer.fileSize();
    }

    /**
     * Get the difference between the highest ancient indicator written to the file and the lowest legal ancient indicator for this
     * file. Higher values mean that the upper bound was chosen well.
     */
    public long getUtilizedSpan() {
        return highestAncientIdentifierInFile - descriptor.getLowerBound();
    }

    /**
     * Get the span that is unused in this file. Low values mean that the upperBound was chosen
     * well, resulting in less overlap between files. A value of 0 represents a "perfect" choice.
     */
    public long getUnUtilizedSpan() {
        return descriptor.getUpperBound() - highestAncientIdentifierInFile;
    }

    /**
     * Get the span of ancient indicators that this file can legally contain.
     */
    public long getSpan() {
        return descriptor.getUpperBound() - descriptor.getLowerBound();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return descriptor.toString();
    }
}
