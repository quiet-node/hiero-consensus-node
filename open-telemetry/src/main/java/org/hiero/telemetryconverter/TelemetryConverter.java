package org.hiero.telemetryconverter;

import static org.hiero.telemetryconverter.Utils.OPEN_TELEMETRY_SCHEMA_URL;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import io.opentelemetry.pbj.common.v1.InstrumentationScope;
import io.opentelemetry.pbj.resource.v1.Resource;
import io.opentelemetry.pbj.trace.v1.ResourceSpans;
import io.opentelemetry.pbj.trace.v1.ScopeSpans;
import io.opentelemetry.pbj.trace.v1.Span;
import io.opentelemetry.pbj.trace.v1.Span.SpanKind;
import io.opentelemetry.pbj.trace.v1.Status;
import io.opentelemetry.pbj.trace.v1.Status.StatusCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.hiero.telemetryconverter.model.EventTraceInfo;
import org.hiero.telemetryconverter.model.RoundTraceInfo;
import org.hiero.telemetryconverter.model.TransactionTraceInfo;

/**
 * TelemetryConverter is a utility class for converting telemetry data.
 * <p>
 * To record JFR telemetry, use the following command in project root:
 * <pre>jcmd &lt;PID&gt; JFR.start duration=300s filename=hedera-node/hedera-app/build/node/data/trace.jfr</pre>
 * </p>
 */
@SuppressWarnings("CallToPrintStackTrace")
public final class TelemetryConverter {
    private static final ZoneId SERVER_TIMEZONE = ZoneId.systemDefault();
    private static final Path PROJECT_ROOT = Paths.get("").toAbsolutePath();
    private static final Path JFR_TRACE_DIR = PROJECT_ROOT.resolve("hedera-node/hedera-app/build/node/data/");
    private static final Path BLOCK_FILES_DIR = PROJECT_ROOT.resolve("hedera-node/hedera-app/build/node/data/blockStreams/block-0.0.3");

    /** Cache of all round traces found in the JFR file, keyed by round number. */
    private static final LongObjectHashMap<LongObjectHashMap<RoundTraceInfo>> roundTraces = new LongObjectHashMap<>();
    /** Cache of all event traces found in the JFR file, keyed by event hash. */
    private static final IntObjectHashMap<LongObjectHashMap<EventTraceInfo>> eventTraces = new IntObjectHashMap<>();
    /** Cache of all transaction traces found in the JFR file, keyed by transaction hash. */
    private static final IntObjectHashMap<LongObjectHashMap<TransactionTraceInfo>> transactionTraces = new IntObjectHashMap<>();
    /** Directory to write test output files to */
    private static final Path testOutputDir = PROJECT_ROOT.resolve("build/telemetry");

    public static void main(String[] args) throws IOException {
        System.out.println("TelemetryConverter.main "+Path.of("").toAbsolutePath().toString());
        // clean or create testOutputDir. Delete contents if it exists, otherwise create new directory
        try {
            if (Files.exists(testOutputDir)) {
                try (Stream<Path> childFiles = Files.walk(testOutputDir)) {
                    childFiles.filter(Files::isRegularFile)
                            .forEach(path -> {
                                try {
                                    Files.delete(path);
                                } catch (IOException e) {
                                    System.err.printf("Error deleting file %s: %s%n", path, e.getMessage());
                                }
                            });
                }
            } else {
                Files.createDirectories(testOutputDir);
            }
        } catch (IOException e) {
            System.err.printf("Error creating build directory %s: %s%n", testOutputDir, e.getMessage());
        }
        // start with reading all trace data from JFR file into maps that we can use later
        try (Stream<Path> jfrDirFiles = Files.walk(JFR_TRACE_DIR)) {
            jfrDirFiles
                    .filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".jfr"))
                    .forEach(TelemetryConverter::readJfrFile);
        }
        // now read the block files and match them with the round traces
        try(Stream<Path> paths = java.nio.file.Files.walk(BLOCK_FILES_DIR)) {
            paths.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".blk.gz"))
                    .sorted(Comparator.comparingLong(path -> {
                        // extract the block number from the file name, assuming it is in the format "<blockNum>.blk.gz"
                        String fileName = path.getFileName().toString();
                        int endIndex = fileName.indexOf('.');
                        return Long.parseLong(fileName.substring(0, endIndex));
                    }))
                    .map(path -> {
                        try (var in = new ReadableStreamingData(new GZIPInputStream(Files.newInputStream(path)))) {
                            return convertBlockToTrace(path, Block.PROTOBUF.parse(in));
                        } catch (IOException | ParseException e) {
                            System.err.printf("Error reading block file %s: %s%n", path, e.getMessage());
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .forEach(TelemetryConverter::sendSpans);
        }
    }

    /**
     * Read a single JFR file from single node and populate the roundTraces, eventTraces, and transactionTraces maps.
     * <p>
     * We expect the JFR file to have a name like "trace-node-25.jfr" where 25 is the node ID.
     * </p>
     * @param jfrFile the path to the JFR file
     */
    private static void readJfrFile(Path jfrFile) {
        System.out.println("Reading JFR file: " + jfrFile);
        final String fileName = jfrFile.getFileName().toString();
        final long nodeId = Long.parseLong(fileName.substring(fileName.indexOf("node-") + 5,fileName.indexOf(".jfr")));
        try (var rf = new RecordingFile(jfrFile)) {
            while (rf.hasMoreEvents()) {
                RecordedEvent e = rf.readEvent();
                System.out.printf("%s  start=%s  dur=%s  thread=%s%n",
                        e.getEventType().getName(),
                        e.getStartTime().atZone(SERVER_TIMEZONE),
                        e.getDuration(),
                        e.getThread() == null ? "-" : e.getThread().getJavaName());
                try {
                    switch (e.getEventType().getName()) {
                        case "consensus.Round" -> {
                            final long roundNum = e.getLong("roundNum");
                            final int eventTypeOrdinal = e.getInt("eventType");
                            final RoundTraceInfo roundTraceInfo = new RoundTraceInfo(roundNum,
                                    RoundTraceInfo.EventType.values()[eventTypeOrdinal],
                                    Utils.instantToUnixEpocNanos(e.getStartTime()),
                                    Utils.instantToUnixEpocNanos(e.getEndTime()));
                            final LongObjectHashMap<RoundTraceInfo> roundNumTraces = roundTraces.getIfAbsent(roundNum, LongObjectHashMap::new);
                            roundNumTraces.put(nodeId, roundTraceInfo);
                        }
                        case "consensus.Event" -> {
                            final int eventHash = e.getInt("eventHash");
                            final int eventTypeOrdinal = e.getInt("eventType");
                            final EventTraceInfo eventTraceInfo = new EventTraceInfo(eventHash,
                                    EventTraceInfo.EventType.values()[eventTypeOrdinal],
                                    Utils.instantToUnixEpocNanos(e.getStartTime()),
                                    Utils.instantToUnixEpocNanos(e.getEndTime()));
                            final LongObjectHashMap<EventTraceInfo> eventHashTraces = eventTraces.getIfAbsent(eventHash, LongObjectHashMap::new);
                            eventHashTraces.put(nodeId, eventTraceInfo);
                        }
                        case "cconsensus.Transaction" -> {
                            final int txHash = e.getInt("txHash");
                            final int eventTypeOrdinal = e.getInt("eventType");
                            final TransactionTraceInfo transactionTraceInfo = new TransactionTraceInfo(txHash,
                                    TransactionTraceInfo.EventType.values()[eventTypeOrdinal],
                                    Utils.instantToUnixEpocNanos(e.getStartTime()),
                                    Utils.instantToUnixEpocNanos(e.getEndTime()));
                            final LongObjectHashMap<TransactionTraceInfo> txHashTraces = transactionTraces.getIfAbsent(txHash, LongObjectHashMap::new);
                            txHashTraces.put(nodeId, transactionTraceInfo);
                        }
                        // ignore other events
                    }
                } catch (Exception ex) {
                    System.err.printf("Error processing event %s: %s -> %s%n", e.getEventType().getName(), ex.getMessage(), e);
                }
            }
        } catch (IOException e) {
            System.err.printf("Error reading JFR file %s: %s%n", jfrFile, e.getMessage());
            e.printStackTrace();
        }
    }

    private static List<Span> convertBlockToTrace(Path blockFile, Block block) {
        final List<Span> spans = new ArrayList<>();
        final List<BlockItem> items = block.items();
        BlockHeader header = items.getFirst().blockHeader();
        try {
            // create a digest for creating trace ids
            final MessageDigest digest = MessageDigest.getInstance("MD5");
            // create a trace id based on the block
            final Bytes blockTraceID = Utils.longToHashBytes(digest, header.number());
            for (int i = 0; i < items.size(); i++) {
                BlockItem item = items.get(i);
                if (item.hasRoundHeader()) {
                    // find all round trace info
                    // create a round span
//                    TransactionTraceInfo txInfo = transactionTraces.get(item.transaction().hash());
//                    if (txInfo != null) {
//                        // create a span for the transaction
//                        ScopeSpans.Builder txSpanBuilder = ScopeSpans.newBuilder()
//                                .setScope("Transaction")
//                                .setStartTime(txInfo.startTime())
//                                .setEndTime(txInfo.endTime())
//                                .putAttributes("transactionHash", item.transaction().hash());
//                        spans.add(txSpanBuilder.build());
//                    }
                } else if (item.hasEventHeader()) {
                    // create an event span
//                    EventTraceInfo eventInfo = eventTraces.get(item.event().hash());
//                    if (eventInfo != null) {
//                        // create a span for the event
//                        ScopeSpans.Builder eventSpanBuilder = ScopeSpans.newBuilder()
//                                .setScope("Event")
//                                .setStartTime(eventInfo.startTime())
//                                .setEndTime(eventInfo.endTime())
//                                .putAttributes("eventHash", item.event().hash());
//                        spans.add(eventSpanBuilder.build());
//                    }
                }
            }
            // create a span for the block
            final Span blockSpan = new Span(
                    blockTraceID, // 16 byte trace id
                    Utils.longToBytes(header.number()), // 8 byte span id
                    null, // don't think we need to use trace state
                    null, // no parent span id
                    0, // TODO flags
                    "Block " + header.number(),
                    SpanKind.SPAN_KIND_CLIENT,
                    123L, // TODO start time, search for time of first transaction in the block
                    Utils.fileCreationTimeEpocNanos(blockFile),
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
            System.err.printf("Error converting block %s: %s%n", header.number(), e.getMessage());
        }
        return spans;
    }

    private static final AtomicLong spanIdCounter = new AtomicLong(0L);

    private static void sendSpans(final List<Span> spans) {
        System.out.println("TelemetryConverter.sendSpans "+spans.size());
        ResourceSpans resourceSpans = new ResourceSpans(
                Resource.newBuilder().build(),
                List.of(ScopeSpans.newBuilder()
                        .scope(InstrumentationScope.newBuilder()
                                .name("Hedera Block Stream")
                                .version("0.65.0")
                                .build())
                        .spans(spans)
                        .schemaUrl(OPEN_TELEMETRY_SCHEMA_URL)
                        .build()),
                OPEN_TELEMETRY_SCHEMA_URL);
        // write json file
        try(WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(
                testOutputDir.resolve("spans"+spanIdCounter.incrementAndGet()+".json")))) {
            ResourceSpans.JSON.write(resourceSpans, out);
        } catch (IOException e) {
            System.err.printf("Error writing spans to file: %s%n", e.getMessage());
        }
    }

}