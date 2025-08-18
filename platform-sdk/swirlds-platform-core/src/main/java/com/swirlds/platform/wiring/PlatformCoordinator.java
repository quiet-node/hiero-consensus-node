// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.wiring;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.swirlds.common.io.IOIterator;
import com.swirlds.common.stream.RunningEventHashOverride;
import com.swirlds.platform.components.AppNotifier;
import com.swirlds.platform.components.EventWindowManager;
import com.swirlds.platform.components.consensus.ConsensusEngine;
import com.swirlds.platform.event.branching.BranchDetector;
import com.swirlds.platform.event.branching.BranchReporter;
import com.swirlds.platform.event.deduplication.EventDeduplicator;
import com.swirlds.platform.event.orphan.OrphanBuffer;
import com.swirlds.platform.listeners.ReconnectCompleteNotification;
import com.swirlds.platform.publisher.PlatformPublisher;
import com.swirlds.platform.state.iss.IssDetector;
import com.swirlds.platform.state.signed.ReservedSignedState;
import com.swirlds.platform.state.signed.SignedState;
import com.swirlds.platform.state.signed.StateSignatureCollector;
import com.swirlds.platform.state.snapshot.StateDumpRequest;
import com.swirlds.platform.system.status.StatusActionSubmitter;
import com.swirlds.platform.system.status.actions.PlatformStatusAction;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import org.hiero.consensus.event.creator.impl.EventCreationManager;
import org.hiero.consensus.event.creator.impl.pool.TransactionPool;
import org.hiero.consensus.event.creator.impl.stale.StaleEventDetector;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.EventWindow;
import org.hiero.consensus.roster.RosterHistory;

/**
 * Responsible for coordinating the clearing of the platform wiring objects.
 */
public class PlatformCoordinator implements StatusActionSubmitter {

    private final PlatformWiring platformWiring;

    public PlatformCoordinator(PlatformWiring platformWiring) {
        this.platformWiring = Objects.requireNonNull(platformWiring);
    }

    /**
     * Flushes the intake pipeline. After this method is called, all components in the intake pipeline (i.e. components
     * prior to the consensus engine) will have been flushed. Additionally, things will be flushed an order that
     * guarantees that there will be no remaining work in the intake pipeline (as long as there are no additional events
     * added to the intake pipeline, and as long as there are no events released by the orphan buffer).
     */
    public void flushIntakePipeline() {
        // Important: the order of the lines within this function matters. Do not alter the order of these
        // lines without understanding the implications of doing so. Consult the wiring diagram when deciding
        // whether to change the order of these lines.

        platformWiring.eventHasherWiring.flush();
        platformWiring.internalEventValidatorWiring.flush();
        platformWiring.eventDeduplicatorWiring.flush();
        platformWiring.eventSignatureValidatorWiring.flush();
        platformWiring.orphanBufferWiring.flush();
        if (platformWiring.pcesInlineWriterWiring != null) {
            platformWiring.pcesInlineWriterWiring.flush();
        }
        platformWiring.gossipWiring.flush();
        platformWiring.consensusEngineWiring.flush();
        platformWiring.applicationTransactionPrehandlerWiring.flush();
        platformWiring.eventCreationManagerWiring.flush();
        platformWiring.branchDetectorWiring.flush();
    }

    /**
     * Safely clears the system in preparation for reconnect. After this method is called, there should be no work
     * sitting in any of the wiring queues, and all internal data structures within wiring components that need to be
     * cleared to prepare for a reconnect should be cleared.
     */
    public void clear() {
        // Important: the order of the lines within this function are important. Do not alter the order of these
        // lines without understanding the implications of doing so. Consult the wiring diagram when deciding
        // whether to change the order of these lines.

        // Phase 0: flush the status state machine.
        // When reconnecting, this will force us to adopt a status that will halt event creation and gossip.
        platformWiring.statusStateMachineWiring.flush();

        // Phase 1: squelch
        // Break cycles in the system. Flush squelched components just in case there is a task being executed when
        // squelch is activated.
        platformWiring.consensusEngineWiring.startSquelching();
        platformWiring.consensusEngineWiring.flush();
        platformWiring.eventCreationManagerWiring.startSquelching();
        platformWiring.eventCreationManagerWiring.flush();
        platformWiring.staleEventDetectorWiring.startSquelching();

        // Also squelch the transaction handler. It isn't strictly necessary to do this to prevent dataflow through
        // the system, but it prevents the transaction handler from wasting time handling rounds that don't need to
        // be handled.
        platformWiring.transactionHandlerWiring.startSquelching();
        platformWiring.transactionHandlerWiring.flush();

        // Phase 2: flush
        // All cycles have been broken via squelching, so now it's time to flush everything out of the system.
        flushIntakePipeline();
        platformWiring.stateHasherWiring.flush();
        platformWiring.stateSignatureCollectorWiring.flush();
        platformWiring.transactionHandlerWiring.flush();
        platformWiring.staleEventDetectorWiring.flush();
        platformWiring.branchDetectorWiring.flush();
        platformWiring.branchReporterWiring.flush();

        // Phase 3: stop squelching
        // Once everything has been flushed out of the system, it's safe to stop squelching.
        platformWiring.consensusEngineWiring.stopSquelching();
        platformWiring.eventCreationManagerWiring.stopSquelching();
        platformWiring.transactionHandlerWiring.stopSquelching();
        platformWiring.staleEventDetectorWiring.stopSquelching();

        // Phase 4: clear
        // Data is no longer moving through the system. Clear all the internal data structures in the wiring objects.
        platformWiring
                .eventDeduplicatorWiring
                .getInputWire(EventDeduplicator::clear)
                .inject(NoInput.getInstance());
        platformWiring.orphanBufferWiring.getInputWire(OrphanBuffer::clear).inject(NoInput.getInstance());
        platformWiring.gossipWiring.getClearInput().inject(NoInput.getInstance());
        platformWiring
                .stateSignatureCollectorWiring
                .getInputWire(StateSignatureCollector::clear)
                .inject(NoInput.getInstance());
        platformWiring
                .eventCreationManagerWiring
                .getInputWire(EventCreationManager::clear)
                .inject(NoInput.getInstance());
        platformWiring
                .staleEventDetectorWiring
                .getInputWire(StaleEventDetector::clear)
                .inject(NoInput.getInstance());
        platformWiring
                .transactionPoolWiring
                .getInputWire(TransactionPool::clear)
                .inject(NoInput.getInstance());
        platformWiring.branchDetectorWiring.getInputWire(BranchDetector::clear).inject(NoInput.getInstance());
        platformWiring.branchReporterWiring.getInputWire(BranchReporter::clear).inject(NoInput.getInstance());
    }

    /**
     * Start gossiping.
     */
    public void startGossip() {
        platformWiring.gossipWiring.startGossip();
    }

    /**
     * Inject a new consensus snapshot into all components that need it. This will happen at restart and reconnect
     * boundaries.
     *
     * @param consensusSnapshot the new consensus snapshot
     */
    public void consensusSnapshotOverride(@NonNull final ConsensusSnapshot consensusSnapshot) {
        platformWiring
                .consensusEngineWiring
                .getInputWire(ConsensusEngine::outOfBandSnapshotUpdate)
                .inject(consensusSnapshot);

        if (platformWiring.publishSnapshotOverrides) {
            platformWiring
                    .platformPublisherWiring
                    .getInputWire(PlatformPublisher::publishSnapshotOverride)
                    .inject(consensusSnapshot);
        }
    }

    /**
     * Flush the transaction handler.
     */
    public void flushTransactionHandler() {
        platformWiring.transactionHandlerWiring.flush();
    }

    /**
     * Flush the state hasher.
     */
    public void flushStateHasher() {
        platformWiring.stateHasherWiring.flush();
    }

    public void resumeGossip() {
        platformWiring.gossipWiring.resumeGossip();
    }

    public void pauseGossip() {
        platformWiring.gossipWiring.pauseGossip();
    }

    /**
     * Forward a state to the hash logger.
     *
     * @param signedState the state to forward
     */
    public void sendStateToHashLogger(@NonNull final SignedState signedState) {
        if (signedState.getState().getHash() != null) {
            final ReservedSignedState stateReservedForHasher = signedState.reserve("logging state hash");

            final boolean offerResult = platformWiring.getHashLoggerInput().offer(stateReservedForHasher);
            if (!offerResult) {
                stateReservedForHasher.close();
            }
        }
    }

    public void sendStateToSignatureCollector(@NonNull final ReservedSignedState reservedState) {
        platformWiring.getSignatureCollectorStateInput().put(reservedState);
    }

    @Override
    public void submitStatusAction(@NonNull final PlatformStatusAction action) {
        platformWiring.getStatusActionSubmitter().submitStatusAction(action);
    }

    /**
     * Update the running hash for all components that need it.
     *
     * @param runningHashUpdate the object containing necessary information to update the running hash
     */
    public void updateRunningHash(@NonNull final RunningEventHashOverride runningHashUpdate) {
        platformWiring.runningEventHashOverrideWiring.runningHashUpdateInput().inject(runningHashUpdate);
    }

    /**
     * Pass an overriding state to the ISS detector.
     *
     * @param state the overriding state
     */
    public void overrideIssDetectorState(@NonNull final ReservedSignedState state) {
        platformWiring
                .issDetectorWiring
                .getInputWire(IssDetector::overridingState)
                .put(state);
    }

    /**
     * Signal the end of the preconsensus replay to the ISS detector.
     */
    public void signalEndOfPcesReplay() {
        platformWiring
                .issDetectorWiring
                .getInputWire(IssDetector::signalEndOfPreconsensusReplay)
                .put(NoInput.getInstance());
    }
    /**
     * Inject a new event window into all components that need it.
     *
     * @param eventWindow the new event window
     */
    public void updateEventWindow(@NonNull final EventWindow eventWindow) {
        // Future work: this method can merge with consensusSnapshotOverride
        platformWiring
                .eventWindowManagerWiring
                .getInputWire(EventWindowManager::updateEventWindow)
                .inject(eventWindow);

        platformWiring
                .staleEventDetectorWiring
                .getInputWire(StaleEventDetector::setInitialEventWindow)
                .inject(eventWindow);

        // Since there is asynchronous access to the shadowgraph, it's important to ensure that
        // it has fully ingested the new event window before continuing.
        platformWiring.gossipWiring.flush();
    }

    /**
     * Start the wiring framework.
     */
    public void start() {
        platformWiring.model.start();
    }

    /**
     * Stop the wiring framework.
     */
    public void stop() {
        platformWiring.model.stop();
    }

    public void setPcesMinimumGeneration(final long minimumGenerationNonAncientForOldestState) {
        platformWiring.getPcesMinimumGenerationToStoreInput().inject(minimumGenerationNonAncientForOldestState);
    }

    public void updateRosterHistory(final RosterHistory rosterHistory) {
        platformWiring.getRosterHistoryInput().inject(rosterHistory);
    }

    public void updatePcesDiscontinuityRound(final long round) {
        platformWiring.getPcesWriterRegisterDiscontinuityInput().inject(round);
    }

    public void notifyListeners(final SignedState signedState) {
        platformWiring
                .getNotifierWiring()
                .getInputWire(AppNotifier::sendReconnectCompleteNotification)
                .put(new ReconnectCompleteNotification(
                        signedState.getRound(), signedState.getConsensusTimestamp(), signedState.getState()));
    }

    public void replayPces(final IOIterator<PlatformEvent> iterator) {
        platformWiring.getPcesReplayerIteratorInput().inject(iterator);
    }

    public void dumpStateToDisk(final StateDumpRequest request) {
        platformWiring.getDumpStateToDiskInput().put(request);
    }
}
