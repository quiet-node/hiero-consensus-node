package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.Roster;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.config.api.Configuration;
import com.swirlds.platform.Consensus;
import com.swirlds.platform.ConsensusImpl;
import com.swirlds.platform.event.linking.SimpleLinker;
import com.swirlds.platform.event.orphan.DefaultOrphanBuffer;
import com.swirlds.platform.event.orphan.OrphanBuffer;
import com.swirlds.platform.gossip.NoOpIntakeEventCounter;
import com.swirlds.platform.internal.EventImpl;
import com.swirlds.platform.metrics.NoOpConsensusMetrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.hiero.consensus.config.EventConfig;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.ConsensusRound;

public class GuiEventProvider {
    private final Consensus consensus;
    private final SimpleLinker linker;
    private final OrphanBuffer orphanBuffer;
    private final Configuration configuration;

    public GuiEventProvider(
            @NonNull final PlatformContext platformContext,
            @NonNull final Roster roster) {

        this.configuration = Objects.requireNonNull(platformContext.getConfiguration());

        this.consensus = new ConsensusImpl(
                platformContext, new NoOpConsensusMetrics(), roster);
        this.linker =
                new SimpleLinker(configuration.getConfigData(EventConfig.class).getAncientMode());
        this.orphanBuffer = new DefaultOrphanBuffer(platformContext, new NoOpIntakeEventCounter());
    }

    public List<GuiEvent> visualizeConsensus(@NonNull final List<PlatformEvent> events) {
        final List<ConsensusRound> rounds = new ArrayList<>();
        final List<EventImpl> impls = new ArrayList<>();
        for (final PlatformEvent event : events) {
            final PlatformEvent copy = event.copyGossipedData();

            final List<PlatformEvent> nonOrphans = orphanBuffer.handleEvent(copy);
            if(nonOrphans.size() != 1){
                throw new IllegalArgumentException("Event is an orphan");
            }

            // since the gui will modify the event, we need to copy it
            final EventImpl eventImpl = linker.linkEvent(nonOrphans.getFirst());
            impls.add(eventImpl);
            if (eventImpl == null) {
                throw new IllegalArgumentException("Event is not linkable");
            }

            rounds.addAll(consensus.addEvent(eventImpl));
        }

        if (rounds.isEmpty()) {
            return impls.stream().map(GuiEvent::fromEventImpl).toList();
        }

        linker.setNonAncientThreshold(rounds.getLast().getEventWindow().getAncientThreshold());

        return linker.getNonAncientEvents().stream().map(GuiEvent::fromEventImpl).toList();
    }
}
