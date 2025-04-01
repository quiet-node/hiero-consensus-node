// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.test.fixtures.gui;

import com.hedera.hapi.platform.event.GossipEvent;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.platform.TestPlatformContextBuilder;
import com.swirlds.platform.test.fixtures.event.generator.GraphGenerator;
import com.swirlds.platform.test.fixtures.event.generator.StandardGraphGenerator;
import com.swirlds.platform.test.fixtures.event.source.EventSource;
import com.swirlds.platform.test.fixtures.event.source.ForkingEventSource;
import com.swirlds.platform.test.fixtures.event.source.StandardEventSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.hiero.consensus.model.event.PlatformEvent;

public class HashgraphGui {
    public static void main(final String[] args) {
        final Randotron randotron = Randotron.create(1);
        final int numNodes = 4;
        final int initialEvents = 2;

        final PlatformContext platformContext =
                TestPlatformContextBuilder.create().build();

        final Map<GossipEvent, Integer> branchIndexMap = new HashMap<>();
        final Map<GossipEvent, Boolean> isSingleEventInBranchMap = new HashMap<>();
        final GraphGenerator graphGenerator =
                new StandardGraphGenerator(platformContext, randotron.nextInt(), generateSources(numNodes, branchIndexMap,
                        isSingleEventInBranchMap));
        graphGenerator.reset();

        final TestGuiSource guiSource = new TestGuiSource(
                platformContext, graphGenerator.getAddressBook(), new GeneratorEventProvider(graphGenerator), branchIndexMap,
                isSingleEventInBranchMap);
        guiSource.generateEvents(initialEvents);
        guiSource.runGui();
    }

    private static @NonNull List<EventSource> generateSources(final int numNetworkNodes, final Map<GossipEvent, Integer> branchIndexMap,
            final Map<GossipEvent, Boolean> isSingleEventInBranchMap) {
        final List<EventSource> list = new LinkedList<>();
        for (long i = 0; i < numNetworkNodes; i++) {
            if (i==1) {
//                list.add(new ForkingEventSource(branchIndexMap, isSingleEventInBranchMap).setForkProbability(0.5).setMaximumBranchCount(2));
                list.add(new ForkingEventSource(branchIndexMap, isSingleEventInBranchMap).setForkProbability(0.5).setMaximumBranchCount(2));
                continue;
            }
            list.add(new StandardEventSource(true));
        }
        return list;
    }
}
