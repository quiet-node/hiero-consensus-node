package org.hiero.telemetryconverter.spancreators;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.opentelemetry.pbj.common.v1.AnyValue;
import io.opentelemetry.pbj.common.v1.KeyValue;
import io.opentelemetry.pbj.trace.v1.Span;
import io.opentelemetry.pbj.trace.v1.Span.Event;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import org.hiero.telemetryconverter.model.combined.BlockInfo;
import org.hiero.telemetryconverter.util.Utils;

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
            for (var round : blockInfo.rounds()) {
                final Bytes roundSpanID = Utils.longToHash8Bytes(round.roundNumber());
                List<Event> events = new ArrayList<>();
                // add events for round created on each node
                for (var createdTrace : round.createdTraces()) {
                    events.add(Event.newBuilder()
                            .name("Round Created on Node " + createdTrace.nodeId())
                            .timeUnixNano(createdTrace.startTimeNanos())
                            .build());
                }
                final Span roundSpan = Span.newBuilder()
                        .traceId(blockTraceID) // 16 byte trace id
                        .spanId(roundSpanID) // 8 byte span id
                        .parentSpanId(blockSpanID)
                        .name("Round " + round.roundNumber()+"  ")
                        .startTimeUnixNano(round.roundStartTimeNanos())
                        .endTimeUnixNano(round.roundEndTimeNanos())
                        .events(events)
                        .build();
                spans.add(roundSpan);
                // create spans for each event in the round
                for (var event : round.events()) {
                    final Bytes eventSpanID = Utils.longToHash8Bytes(event.eventHash());
                    final Span eventSpan = Span.newBuilder()
                            .traceId(blockTraceID) // 16 byte trace id
                            .spanId(eventSpanID) // 8 byte span id
                            .parentSpanId(roundSpanID)
                            .name("Event " + Integer.toHexString(event.eventHash()))
                            .startTimeUnixNano(event.eventStartTimeNanos())
                            .endTimeUnixNano(event.eventEndTimeNanos())
                            .attributes(
                                    KeyValue.newBuilder()
                                            .key("no of transactions")
                                            .value(AnyValue.newBuilder()
                                                    .intValue(event.transactions().size())
                                                    .build())
                                            .build()
                            )
                            .build();
                    spans.add(eventSpan);
                }
            }
            // create a span for the block
            final Span blockSpan = Span.newBuilder()
                    .traceId(blockTraceID) // 16 byte trace id
                    .spanId(blockSpanID) // 8 byte span id
                    .name("Block " + blockInfo.blockNum())
                    .startTimeUnixNano(blockInfo.blockStartTimeNanos())
                    .endTimeUnixNano(blockInfo.blockEndTimeNanos())
                    .build();
            spans.add(blockSpan);
        } catch (Exception e) {
            System.err.printf("Error converting block %s: %s%n", blockInfo.blockNum(), e.getMessage());
        }
        return spans;
    }
}
