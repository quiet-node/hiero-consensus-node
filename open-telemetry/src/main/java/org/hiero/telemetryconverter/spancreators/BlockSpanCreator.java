package org.hiero.telemetryconverter.spancreators;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.opentelemetry.pbj.trace.v1.Span;
import io.opentelemetry.pbj.trace.v1.Span.SpanKind;
import io.opentelemetry.pbj.trace.v1.Status;
import io.opentelemetry.pbj.trace.v1.Status.StatusCode;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hiero.telemetryconverter.Utils;
import org.hiero.telemetryconverter.model.combined.BlockInfo;

/**
 * Create trace spans for a block, from the perspective of the block.
 */
public class BlockSpanCreator {
    public static List<Span> createBlockSpans(final BlockInfo blockInfo) {
        final List<Span> spans = new ArrayList<>();
        try {
            // create a digest for creating trace ids
            final MessageDigest digest = MessageDigest.getInstance("MD5");
            // create a trace id and span id based on the block
            final Bytes blockTraceID = Utils.longToHash16Bytes(digest, blockInfo.blockNum());
            final Bytes blockSpanID = Utils.longToHash8Bytes(blockInfo.blockNum());
            // create spans for each round in the block
            // create a span for the block
            final Span blockSpan = new Span(
                    blockTraceID, // 16 byte trace id
                    blockSpanID, // 8 byte span id
                    null, // don't think we need to use trace state
                    null, // no parent span id
                    0, // TODO flags
                    "Block " + blockInfo.blockNum(),
                    SpanKind.SPAN_KIND_CLIENT,
                    blockInfo.blockStartTimeNanos(),
                    blockInfo.blockEndTimeNanos(),
                    Collections.emptyList(), // TODO attributes
                    0,
                    Collections.emptyList(), // TODO events
                    0,
                    Collections.emptyList(), // TODO links
                    0,
                    new Status(null, StatusCode.STATUS_CODE_OK)
            );
            spans.add(blockSpan);
        } catch (Exception e) {
            System.err.printf("Error converting block %s: %s%n", blockInfo.blockNum(), e.getMessage());
        }
        return spans;
    }
}
