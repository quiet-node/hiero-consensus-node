package org.hiero.telemetryconverter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.hiero.telemetryconverter.model.trace.BlockTraceInfo;
import org.hiero.telemetryconverter.model.trace.EventTraceInfo;
import org.hiero.telemetryconverter.model.trace.RoundTraceInfo;
import org.hiero.telemetryconverter.model.trace.TransactionTraceInfo;

public class JfrFileReader {
    /**
     * Read a single JFR file from single node and populate the roundTraces, eventTraces, and transactionTraces maps.
     * <p>
     * We expect the JFR file to have a name like "trace-node-25.jfr" where 25 is the node ID.
     * </p>
     *
     * @param jfrFile the path to the JFR file
     */
    public static void readJfrFile(final Path jfrFile,
            final LongObjectHashMap<List<BlockTraceInfo>> blockTraces,
            final LongObjectHashMap<List<RoundTraceInfo>> roundTraces,
            final IntObjectHashMap<List<EventTraceInfo>> eventTraces,
            final IntObjectHashMap<List<TransactionTraceInfo>> transactionTraces) {
        final String fileName = jfrFile.getFileName().toString();
        final long nodeId = Long.parseLong(fileName.substring(fileName.indexOf("node-") + 5, fileName.indexOf(".jfr")));
        System.out.printf("Reading JFR file: %s , nodeId:%d%n", jfrFile, nodeId);
        try (var rf = new RecordingFile(jfrFile)) {
            while (rf.hasMoreEvents()) {
                final RecordedEvent e = rf.readEvent();
                try {
                    switch (e.getEventType().getName()) {
                        case "consensus.Block" -> {
                            final long blockNum = e.getLong("blockNumber");
                            final int eventTypeOrdinal = e.getInt("eventType");
                            final BlockTraceInfo blockTraceInfo = new BlockTraceInfo(nodeId, blockNum,
                                    BlockTraceInfo.EventType.values()[eventTypeOrdinal],
                                    Utils.instantToUnixEpocNanos(e.getStartTime()),
                                    Utils.instantToUnixEpocNanos(e.getEndTime()));
                            blockTraces.getIfAbsentPut(blockNum, ArrayList::new).add(blockTraceInfo);
                        }
                        case "consensus.Round" -> {
                            final long roundNum = e.getLong("roundNum");
                            final int eventTypeOrdinal = e.getInt("eventType");
                            final RoundTraceInfo roundTraceInfo = new RoundTraceInfo(nodeId, roundNum,
                                    RoundTraceInfo.EventType.values()[eventTypeOrdinal],
                                    Utils.instantToUnixEpocNanos(e.getStartTime()),
                                    Utils.instantToUnixEpocNanos(e.getEndTime()));
                            roundTraces.getIfAbsentPut(roundNum, ArrayList::new).add(roundTraceInfo);
                        }
                        case "consensus.Event" -> {
                            final int eventHash = e.getInt("eventHash");
                            final int eventTypeOrdinal = e.getInt("eventType");
                            final EventTraceInfo eventTraceInfo = new EventTraceInfo(nodeId, eventHash,
                                    EventTraceInfo.EventType.values()[eventTypeOrdinal],
                                    Utils.instantToUnixEpocNanos(e.getStartTime()),
                                    Utils.instantToUnixEpocNanos(e.getEndTime()));
                            eventTraces.getIfAbsentPut(eventHash, ArrayList::new)
                                    .add(eventTraceInfo);
                        }
                        case "consensus.Transaction" -> {
                            final int txHash = e.getInt("txHash");
                            final int eventTypeOrdinal = e.getInt("eventType");
                            final TransactionTraceInfo transactionTraceInfo = new TransactionTraceInfo(nodeId, txHash,
                                    TransactionTraceInfo.EventType.values()[eventTypeOrdinal],
                                    Utils.instantToUnixEpocNanos(e.getStartTime()),
                                    Utils.instantToUnixEpocNanos(e.getEndTime()));
                            transactionTraces.getIfAbsentPut(txHash, ArrayList::new)
                                    .add(transactionTraceInfo);
                        }
                        // ignore other events
                    }
                } catch (Exception ex) {
                    System.err.printf("Error processing event %s: %s -> %s%n", e.getEventType().getName(),
                            ex.getMessage(), e);
                }
            }
        } catch (IOException e) {
            System.err.printf("Error reading JFR file %s: %s%n", jfrFile, e.getMessage());
            e.printStackTrace();
        }
        final long numberOfRoundTraces = roundTraces.values().stream().mapToLong(List::size).sum();
        final long numberOfEventTraces = eventTraces.values().stream().mapToLong(List::size).sum();
        final long numberOfTransactionTraces = transactionTraces.values().stream()
                .mapToLong(List::size).sum();
        System.out.printf(
                "    Finished reading JFR file: %s, found %,d round traces, %,d event traces, %,d transaction traces%n",
                jfrFile, numberOfRoundTraces, numberOfEventTraces, numberOfTransactionTraces);
    }
}
