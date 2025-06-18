// SPDX-License-Identifier: Apache-2.0
package com.swirlds.state.merkle;

import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Utility class for working with states. */
public final class StateUtils {

    /** Prevent instantiation */
    private StateUtils() {}

    /**
     * Write the {@code object} to the {@link OutputStream} using the given {@link Codec}.
     *
     * @param out The object to write out
     * @param codec The codec to use. MUST be compatible with the {@code object} type
     * @param object The object to write
     * @return The number of bytes written to the stream.
     * @param <T> The type of the object and associated codec.
     * @throws IOException If the output stream throws it.
     * @throws ClassCastException If the object or codec is not for type {@code T}.
     */
    public static <T> int writeToStream(
            @NonNull final OutputStream out, @NonNull final Codec<T> codec, @Nullable final T object)
            throws IOException {
        final var stream = new WritableStreamingData(out);

        final var byteStream = new ByteArrayOutputStream();
        codec.write(object, new WritableStreamingData(byteStream));

        stream.writeInt(byteStream.size());
        stream.writeBytes(byteStream.toByteArray());
        return byteStream.size();
    }

    /**
     * Read an object from the {@link InputStream} using the given {@link Codec}.
     *
     * @param in The input stream to read from
     * @param codec The codec to use. MUST be compatible with the {@code object} type
     * @return The object read from the stream
     * @param <T> The type of the object and associated codec.
     * @throws IOException If the input stream throws it or parsing fails
     * @throws ClassCastException If the object or codec is not for type {@code T}.
     */
    @Nullable
    public static <T> T readFromStream(@NonNull final InputStream in, @NonNull final Codec<T> codec)
            throws IOException {
        final var stream = new ReadableStreamingData(in);
        final var size = stream.readInt();

        stream.limit((long) size + Integer.BYTES); // +4 for the size
        try {
            return codec.parse(stream);
        } catch (final ParseException ex) {
            throw new IOException(ex);
        }
    }


}
