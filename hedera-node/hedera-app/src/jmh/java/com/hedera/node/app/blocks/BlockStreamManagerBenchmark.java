package com.hedera.node.app.blocks;

import static com.hedera.node.app.blocks.schemas.V0560BlockStreamSchema.BLOCK_STREAM_INFO_KEY;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.CryptoTransferOutput;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.block.stream.output.TransactionOutput;
import com.hedera.hapi.block.stream.output.TransactionResult;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.platform.event.EventCore;
import com.hedera.hapi.platform.event.EventTransaction;
import com.hedera.node.app.blocks.impl.BlockStreamManagerImpl;
import com.hedera.node.app.blocks.impl.BoundaryStateChangeListener;
import com.hedera.node.app.config.ConfigProviderImpl;
import com.hedera.node.app.metrics.StoreMetricsServiceImpl;
import com.hedera.node.app.spi.metrics.StoreMetricsService;
import com.hedera.node.app.version.ServicesSoftwareVersion;
import com.hedera.node.config.ConfigProvider;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.common.crypto.Hash;
import com.swirlds.common.merkle.crypto.MerkleCryptography;
import com.swirlds.common.metrics.noop.NoOpMetrics;
import com.swirlds.common.platform.NodeId;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.system.Round;
import com.swirlds.platform.system.SoftwareVersion;
import com.swirlds.platform.system.events.ConsensusEvent;
import com.swirlds.platform.system.state.notifications.StateHashedNotification;
import com.swirlds.platform.system.transaction.ConsensusTransaction;
import com.swirlds.platform.system.transaction.Transaction;
import com.swirlds.state.State;
import com.swirlds.state.spi.CommittableWritableStates;
import com.swirlds.state.spi.ReadableKVState;
import com.swirlds.state.spi.ReadableQueueState;
import com.swirlds.state.spi.ReadableSingletonState;
import com.swirlds.state.spi.ReadableStates;
import com.swirlds.state.spi.WritableKVState;
import com.swirlds.state.spi.WritableQueueState;
import com.swirlds.state.spi.WritableSingletonState;
import com.swirlds.state.spi.WritableStates;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@org.openjdk.jmh.annotations.State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@Fork(1)
public class BlockStreamManagerBenchmark {

    private BlockStreamManagerImpl blockStreamManager;
    private TestState state;
    private TestRound round;
    private ForkJoinPool executor;
    private ConfigProvider configProvider;
    private TestBlockHashSigner blockHashSigner;
    private BoundaryStateChangeListener boundaryStateChangeListener;
    private TestBlockItemWriter writer;
    private ScheduledExecutorService stateHashScheduler;
    private CompletableFuture<Void> stateHashFuture;

    private Instant consensusTime;

    private static BlockStreamInfo blockStreamInfo = BlockStreamInfo.DEFAULT;

    @Param({"10000"})
    public int transactionCount;

    @Param({"10"})
    public int blockCount;

    @Param({"1000"})
    public int stateHashDelayMillis;

    public static void main(String... args) throws Exception {
        org.openjdk.jmh.Main.main(new String[] {"com.hedera.node.app.blocks.BlockStreamManagerBenchmark.benchmarkEndRoundWithAsyncBlockRootHash"});
    }

    public void setup() {
        blockStreamInfo = BlockStreamInfo.DEFAULT;
        executor = new ForkJoinPool(4);
        stateHashScheduler = new ScheduledThreadPoolExecutor(1);
        consensusTime = Instant.now();
        stateHashFuture = CompletableFuture.completedFuture(null);

        this.configProvider = new ConfigProviderImpl(true);

        // Create simple implementations
        blockHashSigner = new TestBlockHashSigner();
        StoreMetricsService storeMetricsService = new StoreMetricsServiceImpl(new NoOpMetrics());
        boundaryStateChangeListener = new BoundaryStateChangeListener(storeMetricsService, () -> configProvider.getConfiguration());
        writer = new TestBlockItemWriter();
        state = new TestState();
        round = new TestRound();

        // Setup InitialStateHash
        var initialStateHash = new InitialStateHash(CompletableFuture.completedFuture(Bytes.wrap(new byte[48])), 0L);

        // Create BlockStreamManager
        blockStreamManager = new BlockStreamManagerImpl(
                blockHashSigner,
                () -> writer,
                executor,
                () -> configProvider.getConfiguration(),
                boundaryStateChangeListener,
                initialStateHash,
                SemanticVersion.DEFAULT,
                new TestPlatformStateFacade(ServicesSoftwareVersion::new));

        // Initialize block hash
        blockStreamManager.initLastBlockHash(BlockStreamManager.ZERO_BLOCK_HASH);
    }

    @TearDown
    public void tearDown() {
        executor.shutdown();
        stateHashScheduler.shutdown();
    }

    @Benchmark
    public void benchmarkEndRoundWithAsyncBlockRootHash() {
        setup();

        // Process multiple blocks in sequence
        for (int blockNum = 1; blockNum < blockCount; blockNum++) {  // Start from block 1 since 0 is initialized
            round.setRoundNum(blockNum);
            round.setConsensusTimestamp(consensusTime);

            //System.out.println("----------------------");
            //System.out.println("Starting Block " + blockNum);
            
            // Start a new round
            blockStreamManager.startRound(round, state);

            //System.out.println("Writing transactions for Block " + blockNum);
            // Write transactions and their associated items to the block
            for (int i = 0; i < transactionCount; i++) {
                // Create and write EventTransaction
                BlockItem eventTxItem = BlockItem.newBuilder()
                    .eventTransaction(EventTransaction.newBuilder()
                        .applicationTransaction(Bytes.wrap("MOCK_TX_" + i))
                        .build())
                    .build();
                blockStreamManager.writeItem(eventTxItem);

                // Create and write TransactionResult
                BlockItem resultItem = BlockItem.newBuilder()
                    .transactionResult(TransactionResult.newBuilder()
                        .transactionFeeCharged(100L)
                        .consensusTimestamp(Timestamp.newBuilder()
                            .seconds(consensusTime.getEpochSecond())
                            .nanos(consensusTime.getNano())
                            .build())
                        .build())
                    .build();
                blockStreamManager.writeItem(resultItem);

                // Create and write TransactionOutput
                BlockItem outputItem = BlockItem.newBuilder()
                    .transactionOutput(TransactionOutput.newBuilder()
                        .cryptoTransfer(new CryptoTransferOutput(List.of()))
                        .build())
                    .build();
                blockStreamManager.writeItem(outputItem);

                // Create and write StateChanges
                BlockItem stateChangesItem = BlockItem.newBuilder()
                    .stateChanges(StateChanges.newBuilder()
                        .consensusTimestamp(Timestamp.newBuilder()
                            .seconds(consensusTime.getEpochSecond())
                            .nanos(consensusTime.getNano())
                            .build())
                        .stateChanges(List.of())
                        .build())
                    .build();
                blockStreamManager.writeItem(stateChangesItem);
            }

           //System.out.println("Finished writing transactions for Block " + blockNum);

            final long currentRound = blockNum;
            final Hash currentHash = state.getHash();

            // Schedule state hash notification with delay and chain it to previous notifications
            stateHashFuture = stateHashFuture.thenRunAsync(() -> {
                try {
                    Thread.sleep(stateHashDelayMillis);
                    blockStreamManager.notify(new StateHashedNotification(currentRound, currentHash));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for state hash delay", e);
                }
            }, executor);

            // End the Block by setting round consensus timestamp
            consensusTime = consensusTime.plusSeconds(2);
            round.setConsensusTimestamp(consensusTime);
            //System.out.println("endRound Block " + blockNum);

            // Now end the round
            boolean endRoundSuccess = blockStreamManager.endRound(state, round.getRoundNum());
            if (!endRoundSuccess) {
                throw new RuntimeException("Failed to end round " + blockNum);
            }
            //System.out.println("endRound Finished Block " + blockNum);
        }

        // Wait for the last state hash notification to complete
        stateHashFuture.join();
    }

    // Simple implementation classes

    private static class TestBlockHashSigner implements BlockHashSigner {
        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public CompletableFuture<Bytes> signFuture(Bytes bytes) {
            return CompletableFuture.completedFuture(Bytes.wrap(new byte[48]));
        }
    }

    private static class TestState implements State {
        private Hash hash = new Hash(new byte[48]);  // Create a non-null hash

        @Override
        public void setHash(Hash hash) {
            this.hash = hash;
        }

        @Override
        public Hash getHash() {
            return hash;
        }

        @Override
        public void init(Time time, Metrics metrics, MerkleCryptography merkleCryptography,
                LongSupplier roundSupplier) {
        }

        @NotNull
        @Override
        public ReadableStates getReadableStates(@NotNull String serviceName) {
            if (BlockStreamService.NAME.equals(serviceName)) {
                return new TestReadableStates();
            }
            return null;
        }

        @NotNull
        @Override
        public WritableStates getWritableStates(@NotNull String serviceName) {
            if (BlockStreamService.NAME.equals(serviceName)) {
                return new TestWritableStates();
            }
            return null;
        }
    }

    private static class TestBlockItemWriter implements BlockItemWriter {

        @Override
        public void openBlock(long blockNumber) {

        }

        @Override
        public void writeItem(@NotNull byte[] bytes) {

        }

        @Override
        public void closeBlock() {

        }
    }

    private static class TestPlatformStateFacade extends PlatformStateFacade {

        /**
         * Create a new instance of {@link PlatformStateFacade}.
         *
         * @param versionFactory a factory to create the current {@link SoftwareVersion} from a {@link SemanticVersion}
         */
        public TestPlatformStateFacade(
                Function<SemanticVersion, SoftwareVersion> versionFactory) {
            super(versionFactory);
        }

        @Override
        public boolean isFreezeRound(@NotNull State state, @NotNull Round round) {
            return false;
        }

        @Override
        public SemanticVersion creationSemanticVersionOf(@NotNull State state) {
            return new SemanticVersion(0, 60, 0, "pre", "build");
        }
    }

    private static class TestWritableStates implements WritableStates, CommittableWritableStates {

        @Override
        public boolean contains(@NotNull String stateKey) {
            return BLOCK_STREAM_INFO_KEY.equals(stateKey);
        }

        @NotNull
        @Override
        public Set<String> stateKeys() {
            return Set.of(BLOCK_STREAM_INFO_KEY);
        }

        @NotNull
        @Override
        public <K, V> WritableKVState<K, V> get(@NotNull String stateKey) {
            return null;
        }

        @NotNull
        @Override
        public <T> WritableSingletonState<T> getSingleton(@NotNull String stateKey) {
            if (BLOCK_STREAM_INFO_KEY.equals(stateKey)) {
                return new WritableSingletonState<>() {
                    @Override
                    public void put(@Nullable T value) {
                        blockStreamInfo = (BlockStreamInfo) value;
                        //System.out.println("Updated BlockStreamInfo: " + value);
                    }

                    @Override
                    public boolean isModified() {
                        return true;
                    }

                    @NotNull
                    @Override
                    public String getStateKey() {
                        return BLOCK_STREAM_INFO_KEY;
                    }

                    @Override
                    public T get() {
                        return (T) blockStreamInfo;
                    }

                    @Override
                    public boolean isRead() {
                        return true;
                    }
                };
            }
            return null;
        }

        @NotNull
        @Override
        public <E> WritableQueueState<E> getQueue(@NotNull String stateKey) {
            return null;
        }

        @Override
        public void commit() {
            //System.out.println("Committing BlockStreamInfo state: " + blockStreamInfo);
        }
    }

    private static class TestReadableStates implements ReadableStates {

        @NotNull
        @Override
        public <K, V> ReadableKVState<K, V> get(@NotNull String stateKey) {
            return null;
        }

        @NotNull
        @Override
        public <T> ReadableSingletonState<T> getSingleton(@NotNull String stateKey) {
            if (BLOCK_STREAM_INFO_KEY.equals(stateKey)) {
                return new ReadableSingletonState<>() {
                    @NotNull
                    @Override
                    public String getStateKey() {
                        return BLOCK_STREAM_INFO_KEY;
                    }

                    @Override
                    public T get() {
                        return (T) blockStreamInfo;
                    }

                    @Override
                    public boolean isRead() {
                        return false;
                    }
                };
            }
            return null;
        }

        @NotNull
        @Override
        public <E> ReadableQueueState<E> getQueue(@NotNull String stateKey) {
            return null;
        }

        @Override
        public boolean contains(@NotNull String stateKey) {
            return false;
        }

        @NotNull
        @Override
        public Set<String> stateKeys() {
            return null;
        }
    }

    private static class TestRound implements Round {
        private long roundNum;
        private Instant consensusTimestamp;

        public void setRoundNum(long roundNum) {
            this.roundNum = roundNum;
        }

        public void setConsensusTimestamp(Instant consensusTimestamp) {
            this.consensusTimestamp = consensusTimestamp;
        }

        @NotNull
        @Override
        public Iterator<ConsensusEvent> iterator() {
            return new Iterator<ConsensusEvent>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public ConsensusEvent next() {
                    return new ConsensusEvent() {
                        @NotNull
                        @Override
                        public Iterator<ConsensusTransaction> consensusTransactionIterator() {
                            return null;
                        }

                        @Override
                        public long getConsensusOrder() {
                            return 0;
                        }

                        @Override
                        public Instant getConsensusTimestamp() {
                            return consensusTimestamp;
                        }

                        @Override
                        public Iterator<Transaction> transactionIterator() {
                            return null;
                        }

                        @Override
                        public Instant getTimeCreated() {
                            return null;
                        }

                        @NotNull
                        @Override
                        public NodeId getCreatorId() {
                            return null;
                        }

                        @NotNull
                        @Override
                        public SemanticVersion getSoftwareVersion() {
                            return null;
                        }

                        @NotNull
                        @Override
                        public EventCore getEventCore() {
                            return null;
                        }

                        @NotNull
                        @Override
                        public Bytes getSignature() {
                            return null;
                        }
                    };
                }
            };
        }

        @Override
        public long getRoundNum() {
            return roundNum;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public int getEventCount() {
            return 0;
        }

        @NotNull
        @Override
        public Roster getConsensusRoster() {
            return null;
        }

        @Override
        public Instant getConsensusTimestamp() {
            return consensusTimestamp;
        }
    }
} 