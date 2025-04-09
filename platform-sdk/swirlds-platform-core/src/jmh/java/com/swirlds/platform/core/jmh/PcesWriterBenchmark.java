// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.core.jmh;

import com.swirlds.common.io.utility.FileUtils;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.platform.TestPlatformContextBuilder;
import com.swirlds.platform.event.preconsensus.FileSyncOption;
import com.swirlds.platform.event.preconsensus.PcesFile;
import com.swirlds.platform.event.preconsensus.PcesFileWriterType;
import com.swirlds.platform.event.preconsensus.PcesMutableFile;
import com.swirlds.platform.test.fixtures.event.TestingEventBuilder;
import com.swirlds.platform.test.fixtures.event.generator.StandardGraphGenerator;
import com.swirlds.platform.test.fixtures.event.source.StandardEventSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
@Measurement(iterations = 5)
public class PcesWriterBenchmark {

    @Param({"OUTPUT_STREAM", "FILE_CHANNEL_DSYNC", "FILE_CHANNEL", "RANDOM_ACCESS_FILE"})
    public PcesFileWriterType writer;

    private Path directory;
    private PcesMutableFile mutableFile;
    static final PlatformEvent EVENT;

    static final int NUM_EVENTS = 100;

    static {

        final Randotron r = Randotron.create(0);
        EVENT  = new TestingEventBuilder(r)
                .setAppTransactionCount(3)
                .setSystemTransactionCount(1)
                .setSelfParent(new TestingEventBuilder(r).build())
                .setOtherParent(new TestingEventBuilder(r).build())
                .build();
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

    //Results on a M1 Max MacBook Pro:
//Benchmark                                      (option)            (writer)   Mode  Cnt        Score        Error  Units
//PcesWriterBenchmark.all                     EVERY_EVENT       OUTPUT_STREAM  thrpt    5      279.472 ±    252.105  ops/s
//PcesWriterBenchmark.all                     EVERY_EVENT  FILE_CHANNEL_DSYNC  thrpt    5        1.827 ±      0.349  ops/s
//PcesWriterBenchmark.all                     EVERY_EVENT        FILE_CHANNEL  thrpt    5        1.789 ±      0.450  ops/s
//PcesWriterBenchmark.all                     EVERY_EVENT  RANDOM_ACCESS_FILE  thrpt    5      265.936 ±    354.480  ops/s
//PcesWriterBenchmark.all                EVERY_SELF_EVENT       OUTPUT_STREAM  thrpt    5      237.601 ±    522.023  ops/s
//PcesWriterBenchmark.all                EVERY_SELF_EVENT  FILE_CHANNEL_DSYNC  thrpt    5        7.261 ±      1.555  ops/s
//PcesWriterBenchmark.all                EVERY_SELF_EVENT        FILE_CHANNEL  thrpt    5        7.513 ±      1.708  ops/s
//PcesWriterBenchmark.all                EVERY_SELF_EVENT  RANDOM_ACCESS_FILE  thrpt    5      396.172 ±    294.832  ops/s
//PcesWriterBenchmark.writeAndSyncEvent               N/A       OUTPUT_STREAM  thrpt    5    20086.463 ±  24514.189  ops/s
//PcesWriterBenchmark.writeAndSyncEvent               N/A  FILE_CHANNEL_DSYNC  thrpt    5      180.130 ±     44.120  ops/s
//PcesWriterBenchmark.writeAndSyncEvent               N/A        FILE_CHANNEL  thrpt    5      181.301 ±     46.327  ops/s
//PcesWriterBenchmark.writeAndSyncEvent               N/A  RANDOM_ACCESS_FILE  thrpt    5    26988.064 ±  42397.662  ops/s
//PcesWriterBenchmark.writeEvent                      N/A       OUTPUT_STREAM  thrpt    5  1313684.065 ±  95200.365  ops/s
//PcesWriterBenchmark.writeEvent                      N/A  FILE_CHANNEL_DSYNC  thrpt    5    19961.494 ±  17596.217  ops/s
//PcesWriterBenchmark.writeEvent                      N/A        FILE_CHANNEL  thrpt    5   543818.942 ± 115003.139  ops/s
//PcesWriterBenchmark.writeEvent                      N/A  RANDOM_ACCESS_FILE  thrpt    5   206974.085 ±  21330.817  ops/s

    //Results ubuntu 22 Intel i7
    // Benchmark                                      (option)            (writer)   Mode  Cnt       Score       Error  Units
    //PcesWriterBenchmark.all                     EVERY_EVENT       OUTPUT_STREAM  thrpt    5      18.436 ±     0.535  ops/s
    //PcesWriterBenchmark.all                     EVERY_EVENT  FILE_CHANNEL_DSYNC  thrpt    5      16.365 ±     0.480  ops/s
    //PcesWriterBenchmark.all                     EVERY_EVENT        FILE_CHANNEL  thrpt    5      18.322 ±     1.089  ops/s
    //PcesWriterBenchmark.all                     EVERY_EVENT  RANDOM_ACCESS_FILE  thrpt    5      18.314 ±     0.824  ops/s
    //PcesWriterBenchmark.all                EVERY_SELF_EVENT       OUTPUT_STREAM  thrpt    5      74.946 ±     2.186  ops/s
    //PcesWriterBenchmark.all                EVERY_SELF_EVENT  FILE_CHANNEL_DSYNC  thrpt    5      18.085 ±     0.278  ops/s
    //PcesWriterBenchmark.all                EVERY_SELF_EVENT        FILE_CHANNEL  thrpt    5      75.378 ±     1.681  ops/s
    //PcesWriterBenchmark.all                EVERY_SELF_EVENT  RANDOM_ACCESS_FILE  thrpt    5      72.974 ±     5.835  ops/s
    //PcesWriterBenchmark.writeAndSyncEvent               N/A       OUTPUT_STREAM  thrpt    5    1849.272 ±    26.053  ops/s
    //PcesWriterBenchmark.writeAndSyncEvent               N/A  FILE_CHANNEL_DSYNC  thrpt    5    1644.452 ±    41.744  ops/s
    //PcesWriterBenchmark.writeAndSyncEvent               N/A        FILE_CHANNEL  thrpt    5    1854.448 ±    11.100  ops/s
    //PcesWriterBenchmark.writeAndSyncEvent               N/A  RANDOM_ACCESS_FILE  thrpt    5    1825.233 ±   111.043  ops/s
    //PcesWriterBenchmark.writeEvent                      N/A       OUTPUT_STREAM  thrpt    5  618148.642 ± 47736.304  ops/s
    //PcesWriterBenchmark.writeEvent                      N/A  FILE_CHANNEL_DSYNC  thrpt    5    1850.401 ±    84.902  ops/s
    //PcesWriterBenchmark.writeEvent                      N/A        FILE_CHANNEL  thrpt    5  798976.557 ± 47014.755  ops/s
    //PcesWriterBenchmark.writeEvent                      N/A  RANDOM_ACCESS_FILE  thrpt    5  328733.917 ±  7864.738  ops/s

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

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void all(BenchmarkState state) throws IOException {
        for (var event : state.events) {
            mutableFile.writeEvent(EVENT);
            if (state.option == FileSyncOption.EVERY_EVENT
                    || event.getCreatorId().equals(state.selfId) && state.option == FileSyncOption.EVERY_SELF_EVENT) {
                mutableFile.sync();
            }
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"EVERY_EVENT", "EVERY_SELF_EVENT"})
        private FileSyncOption option;
        private NodeId selfId = NodeId.FIRST_NODE_ID;
         private List<PlatformEvent> events;

        @Setup(Level.Iteration)
        public void setup() throws IOException, InterruptedException {
            final Randotron r = Randotron.create(0);
            final StandardGraphGenerator generator = new StandardGraphGenerator(
                    TestPlatformContextBuilder.create().build(),
                    r.nextLong(),
                    new StandardEventSource(),
                    new StandardEventSource(),
                    new StandardEventSource(),
                    new StandardEventSource());

            events = new ArrayList<>();
            for (int i = 0; i < NUM_EVENTS; i++) {
                events.add(generator.generateEvent().getBaseEvent());
            }

            System.out.println("selfEvents:"
                    + events.stream()
                    .filter(event -> event.getCreatorId().equals(selfId))
                    .count());
            Collections.shuffle(events);
        }
    }
}
