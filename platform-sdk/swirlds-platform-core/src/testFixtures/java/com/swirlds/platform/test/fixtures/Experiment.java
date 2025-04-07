// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.test.fixtures;

import com.google.common.collect.ImmutableList;
import com.swirlds.common.io.utility.FileUtils;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.platform.TestPlatformContextBuilder;
import com.swirlds.platform.event.preconsensus.PcesFile;
import com.swirlds.platform.event.preconsensus.PcesFileWriterType;
import com.swirlds.platform.event.preconsensus.PcesMutableFile;
import com.swirlds.platform.test.fixtures.event.TestingEventBuilder;
import com.swirlds.platform.test.fixtures.event.generator.StandardGraphGenerator;
import com.swirlds.platform.test.fixtures.event.source.StandardEventSource;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.hiero.consensus.model.event.AncientMode;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.node.NodeId;

public class Experiment {
    private static final boolean SLEEP = false;
    public static PcesFileWriterType fileWriterType = PcesFileWriterType.FILE_CHANNEL;

    private static Path directory;
    private static PcesMutableFile mutableFile;
    static final List<PlatformEvent> EVENTS;
    static final PlatformEvent event;
    static final NodeId selfId = NodeId.FIRST_NODE_ID;

    static final int NUM_EVENTS = 100;

    static {
        final Randotron r = Randotron.create(0);
        EVENTS = List.of();
        event = new TestingEventBuilder(r)
                .setAppTransactionCount(3)
                .setSystemTransactionCount(1)
                .setSelfParent(new TestingEventBuilder(r).build())
                .setOtherParent(new TestingEventBuilder(r).build())
                .build();
    }

    private static List<PlatformEvent> extracted(final Randotron r) {
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
        System.out.println("selfEvents:"
                + EVENTS.stream()
                        .filter(event -> event.getCreatorId().equals(selfId))
                        .count());

        return ImmutableList.copyOf(events);
    }

    public static void setup() throws IOException {

        final Randotron r = Randotron.create(0);

        directory = Files.createTempDirectory("PcesWriterBenchmark");
        final PcesFile file = PcesFile.of(AncientMode.GENERATION_THRESHOLD, r.nextInstant(), 1, 0, 100, 0, directory);

        mutableFile = file.getMutableFile(fileWriterType);
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        try {
            setup();
            System.out.println("one minute to start tracing");
            extracted();
            writeEvents();
            System.out.println(LocalDateTime.now() + " stop tracing");
            extracted();
        } finally {
            cleanup();
        }
    }

    private static void extracted() throws InterruptedException {
        if (SLEEP) {
            Thread.sleep(50000);
        }
    }

    public static void writeEvents() throws IOException {
        final var start = System.currentTimeMillis();
        System.out.println(LocalDateTime.now() + " write");
        mutableFile.writeEvent(event);
        System.out.println(LocalDateTime.now() + " sync");
        mutableFile.sync();
        System.out.println("elapsed time:" + (System.currentTimeMillis() - start));
    }

    public static void cleanup() throws IOException {
        mutableFile.close();
        FileUtils.deleteDirectory(directory);
    }
}
