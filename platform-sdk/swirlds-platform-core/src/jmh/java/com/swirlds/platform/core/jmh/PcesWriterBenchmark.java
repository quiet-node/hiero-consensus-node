// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.core.jmh;

import com.google.common.collect.ImmutableList;
import com.swirlds.common.io.utility.FileUtils;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.platform.TestPlatformContextBuilder;
import com.swirlds.platform.event.preconsensus.PcesFile;
import com.swirlds.platform.event.preconsensus.PcesFileWriterType;
import com.swirlds.platform.event.preconsensus.PcesMutableFile;
import com.swirlds.platform.test.fixtures.event.generator.StandardGraphGenerator;
import com.swirlds.platform.test.fixtures.event.source.StandardEventSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hiero.consensus.model.event.AncientMode;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.node.NodeId;
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
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 10)
public class PcesWriterBenchmark {

    @Param({"OUTPUT_STREAM", "FILE_CHANNEL_DSYNC", "FILE_CHANNEL", "RANDOM_ACCESS_FILE"})
    public PcesFileWriterType writer;

    private Path directory;
    private PcesMutableFile mutableFile;
    static final List<PlatformEvent> EVENTS;
    static final PlatformEvent EVENT;
    static final NodeId selfId = NodeId.FIRST_NODE_ID;

    static final int NUM_EVENTS = 100;

    static {
        final Randotron r = Randotron.create(0);
        final StandardGraphGenerator generator = new StandardGraphGenerator(
                TestPlatformContextBuilder.create().build(),
                r.nextLong(),
                new StandardEventSource(),
                new StandardEventSource(),
                new StandardEventSource(),
                new StandardEventSource());

        final var events = new ArrayList<PlatformEvent>();
        for (int i = 0; i < NUM_EVENTS; i++) {
            events.add(generator.generateEvent().getBaseEvent());
        }
        EVENTS = ImmutableList.copyOf(events);
        EVENT = EVENTS.getFirst();
        System.out.println("selfEvents:"
                + EVENTS.stream()
                        .filter(event -> event.getCreatorId().equals(selfId))
                        .count());
    }

    @Setup(Level.Iteration)
    public void setup() throws IOException, InterruptedException {

        Thread.sleep(10000);
        final Randotron r = Randotron.create(0);

        directory = Files.createTempDirectory("PcesWriterBenchmark");
        final PcesFile file = PcesFile.of(AncientMode.GENERATION_THRESHOLD, r.nextInstant(), 1, 0, 100, 0, directory);

        mutableFile = file.getMutableFile(writer);
    }

    @TearDown(Level.Iteration)
    public void cleanup() throws IOException {
        mutableFile.close();
        FileUtils.deleteDirectory(directory);
    }
    /*
    Results on a M1 Max MacBook Pro:

    Benchmark                       (syncEveryEvent)  (useFileChannelWriter)   Mode  Cnt       Score        Error  Units
    PcesWriterBenchmark.writeEvent              true                    true  thrpt    3   12440.268 ±  42680.146  ops/s
    PcesWriterBenchmark.writeEvent              true                   false  thrpt    3   16244.412 ±  38461.148  ops/s
    PcesWriterBenchmark.writeEvent             false                    true  thrpt    3  411138.079 ± 110692.138  ops/s
    PcesWriterBenchmark.writeEvent             false                   false  thrpt    3  643582.781 ± 154393.415  ops/s
    */

    // Benchmark                       (syncEveryEvent)  (useFileChannelWriter)   Mode  Cnt        Score        Error
    // Units
    // PcesWriterBenchmark.writeEvent              true                    true  thrpt    3    25208.858 ±  99590.982
    // ops/s
    // PcesWriterBenchmark.writeEvent              true                   false  thrpt    3    34301.777 ±  94947.390
    // ops/s
    // PcesWriterBenchmark.writeEvent             false                    true  thrpt    3   513363.285 ± 447530.872
    // ops/s
    // PcesWriterBenchmark.writeEvent             false                   false  thrpt    3  1292696.028 ± 198412.982
    // ops/s

    // Benchmark                       (syncEveryEvent)  (useFileChannelWriter)   Mode  Cnt        Score        Error
    // Units
    // PcesWriterBenchmark.writeEvent              true                    true  thrpt    3   516916.172 ± 499773.057
    // ops/s
    // PcesWriterBenchmark.writeEvent              true                   false  thrpt    3    27711.788 ± 209413.534
    // ops/s
    // PcesWriterBenchmark.writeEvent             false                    true  thrpt    3   541710.710 ± 445208.701
    // ops/s
    // PcesWriterBenchmark.writeEvent             false                   false  thrpt    3  1356841.154 ± 978813.630
    // ops/s
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void writeEvent() throws IOException {
        mutableFile.writeEvent(EVENT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void writeAndSyncEvent() throws IOException {
        mutableFile.writeEvent(EVENT);
        mutableFile.sync();
    }
}
