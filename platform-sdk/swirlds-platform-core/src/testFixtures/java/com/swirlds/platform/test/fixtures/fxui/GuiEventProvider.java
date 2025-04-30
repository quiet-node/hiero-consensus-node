package com.swirlds.platform.test.fixtures.fxui;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.config.api.Configuration;
import com.swirlds.platform.Consensus;
import com.swirlds.platform.ConsensusImpl;
import com.swirlds.platform.consensus.ConsensusConfig;
import com.swirlds.platform.consensus.RoundCalculationUtils;
import com.swirlds.platform.event.linking.SimpleLinker;
import com.swirlds.platform.internal.EventImpl;
import com.swirlds.platform.metrics.NoOpConsensusMetrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import org.hiero.consensus.config.EventConfig;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.ConsensusRound;
import org.hiero.consensus.model.roster.AddressBook;
import org.hiero.consensus.roster.RosterRetriever;

public class GuiEventProvider {
    private final GraphVisualizer graphVisualizer;
    private final Consensus consensus;
    private final SimpleLinker linker;
    private final Configuration configuration;

    /**
     * @param configuration this node's configuration
     * @param addressBook   the network's address book
     */
    public GuiEventProvider(
            @NonNull final Configuration configuration,
            @NonNull final AddressBook addressBook,
            @NonNull final GraphVisualizer graphVisualizer) {

        this.configuration = Objects.requireNonNull(configuration);
        this.graphVisualizer = Objects.requireNonNull(graphVisualizer);
        final PlatformContext platformContext = PlatformContext.create(configuration);

        this.consensus = new ConsensusImpl(
                platformContext, new NoOpConsensusMetrics(), RosterRetriever.buildRoster(addressBook));
        this.linker =
                new SimpleLinker(configuration.getConfigData(EventConfig.class).getAncientMode());
    }

    public void visualizeConsensus(@NonNull final PlatformEvent event) {
        // since the gui will modify the event, we need to copy it
        final EventImpl eventImpl = linker.linkEvent(event.copyGossipedData());
        if (eventImpl == null) {
            throw new IllegalArgumentException("Event is not linkable");
        }
        eventImpl.getBaseEvent().setNGen(event.getNGen());

        final List<ConsensusRound> rounds = consensus.addEvent(eventImpl);

        graphVisualizer.displayEvent(
                GuiEvent.fromEventImpl(eventImpl)
        );

        if (rounds.isEmpty()) {
            return;
        }

        graphVisualizer.displayEvents(linker.getNonAncientEvents().stream().map(GuiEvent::fromEventImpl).toList());

        linker.setNonAncientThreshold(rounds.getLast().getEventWindow().getAncientThreshold());
    }

    /**
     * Handle a consensus snapshot override (i.e. what happens when we start from a node state at restart/reconnect
     * boundaries).
     *
     * @param snapshot the snapshot to handle
     */
    public void handleSnapshotOverride(@NonNull final ConsensusSnapshot snapshot) {
        consensus.loadSnapshot(snapshot);
        linker.clear();
        linker.setNonAncientThreshold(RoundCalculationUtils.getAncientThreshold(
                configuration.getConfigData(ConsensusConfig.class).roundsNonAncient(), snapshot));
    }
}
