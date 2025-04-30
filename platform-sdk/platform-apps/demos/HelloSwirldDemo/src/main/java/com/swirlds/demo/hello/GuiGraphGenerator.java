package com.swirlds.demo.hello;

import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.platform.TestPlatformContextBuilder;
import com.swirlds.platform.internal.EventImpl;
import com.swirlds.platform.test.fixtures.event.generator.GraphGenerator;
import com.swirlds.platform.test.fixtures.event.generator.StandardGraphGenerator;
import com.swirlds.platform.test.fixtures.event.source.EventSource;
import com.swirlds.platform.test.fixtures.event.source.StandardEventSource;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class GuiGraphGenerator {
    private final GraphGenerator graphGenerator;
    private final GuiEventProvider eventProvider;

    public GuiGraphGenerator(final long seed, final int numNodes) {
        final Randotron randotron = Randotron.create(seed);

        final PlatformContext platformContext =
                TestPlatformContextBuilder.create().build();

        this.graphGenerator = new StandardGraphGenerator(
                platformContext,
                randotron.nextInt(),
                generateSources(numNodes));
        this.eventProvider = new GuiEventProvider(
                platformContext,
                graphGenerator.getRoster()
        );
        graphGenerator.reset();
    }

    public List<GuiEvent> generateEvents(final int numEvents) {
        return eventProvider.visualizeConsensus(
                Stream.generate(graphGenerator::generateEvent)
                        .limit(numEvents)
                        .map(EventImpl::getBaseEvent)
                        .toList());
    }

    private static List<EventSource> generateSources(final int numNetworkNodes) {
        final List<EventSource> list = new LinkedList<>();
        for (long i = 0; i < numNetworkNodes; i++) {
            list.add(new StandardEventSource(true));
        }
        return list;
    }
}
