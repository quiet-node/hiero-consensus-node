// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.cache;

import static com.hedera.node.app.blocks.impl.streaming.FileBlockItemWriter.COMPLETE_BLOCK_EXTENSION;
import static com.hedera.node.app.blocks.impl.streaming.FileBlockItemWriter.COMPRESSION_ALGORITHM_EXTENSION;
import static com.hedera.node.app.blocks.impl.streaming.FileBlockItemWriter.longToFileName;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.streams.HashObject;
import com.hedera.node.app.blocks.impl.streaming.BlockState;
import com.hedera.node.app.records.impl.producers.BlockRecordWriter;
import com.hedera.node.app.records.impl.producers.BlockRecordWriterFactory;
import com.hedera.node.app.records.impl.producers.SerializedSingleTransactionRecord;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.data.BlockStreamConfig;
import com.hedera.node.config.data.IssContextConfig;
import com.hedera.node.config.types.StreamMode;
import com.swirlds.state.lifecycle.info.NetworkInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.PublishStreamRequest;

/**
 * An in-memory cache of the last X record stream record files and block stream block files produced
 * by the Execution layer. This class also handles writing these files to disk.
 */
@Singleton
public class ExecutionOutputCache {

    private static final Logger log = LogManager.getLogger(ExecutionOutputCache.class);

    private final EvictingLinkedHashMap<BlockState> blockStreamCache;
    private final EvictingLinkedHashMap<RecordStreamMetadata> recordStreamCache;

    private final ConfigProvider configProvider;
    private final NetworkInfo networkInfo;

    private long issRoundNumber;

    private final BlockRecordWriterFactory blockRecordWriterFactory;

    /**
     * Constructor for ExecutionOutputCache.
     * @param configProvider the configuration provider
     * @param networkInfo the network information to access self node info
     * @param blockRecordWriterFactory the factory to create BlockRecordWriter instances
     */
    @Inject
    public ExecutionOutputCache(
            @NonNull ConfigProvider configProvider,
            @NonNull NetworkInfo networkInfo,
            @NonNull BlockRecordWriterFactory blockRecordWriterFactory) {
        final int capacity = configProvider
                .getConfiguration()
                .getConfigData(IssContextConfig.class)
                .recordBlockBufferSize();
        this.blockStreamCache = new EvictingLinkedHashMap<>(capacity);
        this.recordStreamCache = new EvictingLinkedHashMap<>(capacity);
        this.configProvider = configProvider;
        this.networkInfo = networkInfo;
        this.blockRecordWriterFactory = blockRecordWriterFactory;
    }

    /**
     * Creates a block state in the BlockState buffer for the given block number.
     * @param blockNumber the block number to create a BlockState for
     */
    public void createBlock(long blockNumber) {
        blockStreamCache.put(blockNumber, new BlockState(blockNumber));
    }

    /**
     * Adds a BlockItem to the BlockState for the given block number if it exists.
     * @param blockNumber the block number to which the BlockItem belongs
     * @param blockItem the BlockItem to add to the BlockState
     */
    public void addBlockItem(long blockNumber, @NonNull BlockItem blockItem) {
        final BlockState blockState = blockStreamCache.get(blockNumber);
        if (blockState != null) {
            blockState.addItem(blockItem);
        }
    }

    /**
     * Get the BlockState for a given round number, if it exists.
     * @param roundNumber the round number to look up
     * @return the BlockState for the round number, or null if not found
     */
    public BlockState getBlockStateForRoundNumber(long roundNumber) {
        for (final BlockState blockState : blockStreamCache.values()) {
            if (blockState.getLowestRoundNumber() != null) {
                if (roundNumber >= blockState.getLowestRoundNumber()
                        && roundNumber <= blockState.getHighestRoundNumber()) {
                    return blockState;
                }
            }
        }
        return null;
    }

    /**
     * Writes the block stream Block for the ISS round number to disk.
     */
    public void handleBlockStreamIssBlock() {
        BlockState blockState = getBlockStateForRoundNumber(issRoundNumber);
        if (blockState != null) {
            writeBlockStateToToDisk(blockState);
        } else {
            log.error(
                    "BlockState for round {} in which ISS was reported is not available, skipping writing to disk",
                    issRoundNumber);
        }
    }

    /**
     * Write ISS Contextual Information to disk, including the block stream Block and record stream record files.
     */
    public void handleIssContextualBlocks(final long issRoundNumber) {
        this.issRoundNumber = issRoundNumber;
        StreamMode streamMode = configProvider
                .getConfiguration()
                .getConfigData(BlockStreamConfig.class)
                .streamMode();
        if (streamMode == StreamMode.BOTH || streamMode == StreamMode.BLOCKS) {
            handleBlockStreamIssBlock();
        }
        if (streamMode == StreamMode.BOTH || streamMode == StreamMode.RECORDS) {
            handleRecordStreamIssRecordFiles();
        }
    }

    private void handleRecordStreamIssRecordFiles() {
        if (recordStreamCache.isEmpty()) {
            log.error(
                    "No record stream files found in cache for ISS round {}. Skipping writing to disk.",
                    issRoundNumber);
            return;
        }
        for (final RecordStreamMetadata recordFileMetadata : recordStreamCache.values()) {
            writeRecordFilesToDisk(recordFileMetadata);
        }
    }

    private void writeRecordFilesToDisk(RecordStreamMetadata recordFileMetadata) {
        List<SerializedSingleTransactionRecord> serializedSingleTransactionRecords =
                recordFileMetadata.getRecordItems();
        if (serializedSingleTransactionRecords.isEmpty()) {
            log.error(
                    "No SerializedSingleTransactionRecord's found for Block number {}. Skipping writing to disk.",
                    recordFileMetadata.getBlockNumber());
            return;
        }
        try {
            String recordDir = configProvider
                            .getConfiguration()
                            .getConfigData(IssContextConfig.class)
                            .diskPath() + issRoundNumber + "/node"
                    + networkInfo.selfNodeInfo().nodeId() + "/";
            BlockRecordWriter blockRecordWriter = blockRecordWriterFactory.create(recordDir);
            blockRecordWriter.init(
                    recordFileMetadata.getHapiProtoVersion(),
                    recordFileMetadata.getStartRunningHash(),
                    recordFileMetadata.getConsensusTime(),
                    recordFileMetadata.getBlockNumber());
            for (SerializedSingleTransactionRecord rec : serializedSingleTransactionRecords) {
                blockRecordWriter.writeItem(rec);
            }
            blockRecordWriter.close(recordFileMetadata.getLastRunningHash());

            log.info(
                    "Successfully wrote Record Stream file for Block number {} to disk at path: {}",
                    recordFileMetadata.getBlockNumber(),
                    blockRecordWriter.getBlockRecordFilePath().toAbsolutePath());
        } catch (UncheckedIOException e) {
            log.error(
                    "Failed to write Record Stream file for Block number {} to disk: {}",
                    recordFileMetadata.getBlockNumber(),
                    e.getMessage(),
                    e);
        }
    }

    /**
     * Adds the SerializedSingleTransactionRecord item to the RecordStreamMetadata for the given block number if it exists.
     * @param blockNumber the block number
     * @param item the SerializedSingleTransactionRecord item to add
     */
    public void addRecordStreamItem(long blockNumber, SerializedSingleTransactionRecord item) {
        RecordStreamMetadata recordStreamMetadata = recordStreamCache.get(blockNumber);
        if (recordStreamMetadata != null) {
            recordStreamMetadata.addRecordItem(item);
        }
    }

    /**
     * Closes the record stream file for the given block number by setting the last running hash.
     * @param blockNumber the block number for which to close the record stream file
     * @param lastRunningHash the last running hash to set for the record stream file
     */
    public void closeRecordStreamFile(long blockNumber, HashObject lastRunningHash) {
        RecordStreamMetadata recordStreamMetadata = recordStreamCache.get(blockNumber);
        if (recordStreamMetadata != null) {
            recordStreamMetadata.setLastRunningHash(lastRunningHash);
        }
    }

    /**
     * Creates a new RecordStreamMetadata for the given block number and initializes it with the provided parameters.
     * @param hapiVersion the HAPI version to set for the record stream metadata
     * @param startRunningHash the starting running hash for the record stream metadata
     * @param startConsensusTime the starting consensus time for the record stream metadata
     * @param blockNumber the block number for which to create the record stream metadata
     */
    public void createRecordStreamBlock(
            SemanticVersion hapiVersion, HashObject startRunningHash, Instant startConsensusTime, long blockNumber) {
        requireNonNull(hapiVersion, "hapiVersion must not be null");
        requireNonNull(startRunningHash, "startRunningHash must not be null");
        requireNonNull(startConsensusTime, "startConsensusTime must not be null");

        final RecordStreamMetadata recordStreamMetadata = new RecordStreamMetadata(blockNumber);
        recordStreamMetadata.setStartRunningHash(startRunningHash);
        recordStreamMetadata.setConsensusTime(startConsensusTime);
        recordStreamMetadata.setHapiProtoVersion(hapiVersion);
        recordStreamCache.put(blockNumber, recordStreamMetadata);
    }

    private static class RecordStreamMetadata {
        private final long blockNumber;
        private Instant consensusTime;

        private HashObject startRunningHash;
        private SemanticVersion hapiProtoVersion;
        private final List<SerializedSingleTransactionRecord> recordItems = new ArrayList<>();

        private HashObject lastRunningHash;

        public RecordStreamMetadata(final long blockNumber) {
            this.blockNumber = blockNumber;
        }

        public long getBlockNumber() {
            return blockNumber;
        }

        public void setConsensusTime(Instant consensusTime) {
            this.consensusTime = consensusTime;
        }

        public Instant getConsensusTime() {
            return consensusTime;
        }

        public void addRecordItem(SerializedSingleTransactionRecord item) {
            recordItems.add(item);
        }

        public List<SerializedSingleTransactionRecord> getRecordItems() {
            return recordItems;
        }

        public void setLastRunningHash(HashObject lastRunningHash) {
            this.lastRunningHash = lastRunningHash;
        }

        public HashObject getStartRunningHash() {
            return startRunningHash;
        }

        public void setStartRunningHash(HashObject startRunningHash) {
            this.startRunningHash = startRunningHash;
        }

        public void setHapiProtoVersion(SemanticVersion hapiVersion) {
            this.hapiProtoVersion = hapiVersion;
        }

        public SemanticVersion getHapiProtoVersion() {
            return hapiProtoVersion;
        }

        public HashObject getLastRunningHash() {
            return lastRunningHash;
        }
    }

    private static class EvictingLinkedHashMap<T> {
        private HashMap<Long, T> buffer = null;

        public EvictingLinkedHashMap(int capacity) {
            this.buffer = new LinkedHashMap<>(capacity, 0.75f, true) {
                protected boolean removeEldestEntry(Map.Entry<Long, T> eldest) {
                    return size() > capacity;
                }
            };
        }

        /**
         * Get an existing item by key/index, or null if not present.
         */
        public T get(long key) {
            return buffer.get(key);
        }

        /**
         * Check if the map is empty.
         * @return true if the map is empty, false otherwise
         */
        public boolean isEmpty() {
            return buffer.isEmpty();
        }

        /**
         * Put an item into the map, evicting the oldest item if the capacity is exceeded.
         * @param key the key for the item
         * @param value the item to put into the map
         */
        public void put(long key, T value) {
            requireNonNull(value, "value must not be null");
            buffer.put(key, value);
        }

        /**
         * Get all values in the map.
         * @return a collection of all values in the map
         */
        public Collection<T> values() {
            return buffer.values();
        }
    }

    /**
     * Writes the block state to disk.
     * @param blockState the block state
     */
    public void writeBlockStateToToDisk(@NonNull final BlockState blockState) {
        requireNonNull(blockState, "blockState must not be null");
        requireNonNull(networkInfo, "networkInfo must not be null");
        List<BlockItem> blockItems = new ArrayList<>();

        // Add all BlockItems from the Requests in the BlockState
        List<PublishStreamRequest> requests = blockState.getRequests();
        for (PublishStreamRequest request : requests) {
            if (request.blockItems() != null) {
                BlockItemSet blockItemSet = request.blockItems();
                blockItems.addAll(blockItemSet.blockItems());
            }
        }
        if (blockState.closedTimestamp() == null) {
            blockItems.addAll(blockState.getPendingItems().stream().toList());
        }

        final Block block = Block.newBuilder().items(blockItems).build();

        IssContextConfig issContextConfig = configProvider.getConfiguration().getConfigData(IssContextConfig.class);
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {

            // Serialize the Block object into bytes
            byte[] blockBytes = Block.PROTOBUF.toBytes(block).toByteArray();

            // Write the serialized bytes to the GZIPOutputStream
            gzipOutputStream.write(blockBytes);
            gzipOutputStream.close();

            // Also write Block File to local disk
            Path issBlockFilePath = Paths.get(issContextConfig.diskPath())
                    .resolve(issRoundNumber + "/node"
                            + networkInfo.selfNodeInfo().nodeId() + "/blockStream/"
                            + longToFileName(blockState.blockNumber()) + COMPLETE_BLOCK_EXTENSION
                            + COMPRESSION_ALGORITHM_EXTENSION);
            try {
                // Ensure the parent directories exist
                Files.createDirectories(issBlockFilePath.getParent());
                Files.write(issBlockFilePath, byteArrayOutputStream.toByteArray());

                log.info(
                        "Successfully wrote Block Stream file for Block number {} to disk at path: {}",
                        blockState.blockNumber(),
                        issBlockFilePath.toAbsolutePath());
            } catch (IOException e) {
                log.error(
                        "Failed to write Block {} to local disk at path {}: {}",
                        blockState.blockNumber(),
                        issBlockFilePath.toAbsolutePath(),
                        e.getMessage(),
                        e);
            }
        } catch (IOException e) {
            log.error(
                    "Failed to write Block Stream file for Block number {} to disk: {}",
                    blockState.blockNumber(),
                    e.getMessage(),
                    e);
        }
    }
}
