// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gossip;

import static com.swirlds.logging.legacy.LogMarker.EXCEPTION;
import static org.hiero.consensus.model.hashgraph.ConsensusConstants.ROUND_UNDEFINED;

import com.google.common.collect.ImmutableList;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.node.state.roster.RosterEntry;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.merkle.synchronization.config.ReconnectConfig;
import com.swirlds.common.threading.manager.ThreadManager;
import com.swirlds.common.threading.pool.CachedPoolParallelExecutor;
import com.swirlds.component.framework.model.WiringModel;
import com.swirlds.component.framework.wires.input.BindableInputWire;
import com.swirlds.component.framework.wires.output.StandardOutputWire;
import com.swirlds.platform.Utilities;
import com.swirlds.platform.components.PlatfromReconnecter;
import com.swirlds.platform.config.StateConfig;
import com.swirlds.platform.gossip.shadowgraph.AbstractShadowgraphSynchronizer;
import com.swirlds.platform.gossip.shadowgraph.RpcShadowgraphSynchronizer;
import com.swirlds.platform.gossip.shadowgraph.Shadowgraph;
import com.swirlds.platform.gossip.shadowgraph.ShadowgraphSynchronizer;
import com.swirlds.platform.metrics.ReconnectMetrics;
import com.swirlds.platform.metrics.SyncMetrics;
import com.swirlds.platform.network.PeerCommunication;
import com.swirlds.platform.network.PeerInfo;
import com.swirlds.platform.network.communication.handshake.VersionCompareHandshake;
import com.swirlds.platform.network.protocol.AbstractSyncProtocol;
import com.swirlds.platform.network.protocol.HeartbeatProtocol;
import com.swirlds.platform.network.protocol.Protocol;
import com.swirlds.platform.network.protocol.ProtocolRunnable;
import com.swirlds.platform.network.protocol.ReconnectProtocol;
import com.swirlds.platform.network.protocol.SyncProtocol;
import com.swirlds.platform.network.protocol.rpc.RpcProtocol;
import com.swirlds.platform.reconnect.ReconnectLearnerFactory;
import com.swirlds.platform.reconnect.ReconnectLearnerThrottle;
import com.swirlds.platform.reconnect.ReconnectSyncHelper;
import com.swirlds.platform.reconnect.ReconnectThrottle;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.SwirldStateManager;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.signed.ReservedSignedState;
import com.swirlds.platform.state.signed.SignedState;
import com.swirlds.platform.wiring.NoInput;
import com.swirlds.platform.wiring.components.Gossip;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.base.crypto.CryptoUtils;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.EventWindow;
import org.hiero.consensus.model.node.KeysAndCerts;
import org.hiero.consensus.model.node.NodeId;
import org.hiero.consensus.model.status.PlatformStatus;
import org.hiero.consensus.roster.RosterUtils;

/**
 * Utility class for wiring various subcomponents of gossip module. In particular, it abstracts away specific protocols
 * from network component using them and connects all of these to wiring framework.
 */
public class SyncGossipModular implements Gossip {

    private static final Logger logger = LogManager.getLogger(SyncGossipModular.class);

    private final PeerCommunication network;
    private final ImmutableList<Protocol> protocols;
    private final AbstractSyncProtocol<?> syncProtocol;
    private final AbstractShadowgraphSynchronizer synchronizer;
    private final ReconnectProtocol reconnectProtocol;

    // this is not a nice dependency, should be removed as well as the sharedState
    private Consumer<PlatformEvent> receivedEventHandler;

    /**
     * Builds the gossip engine, depending on which flavor is requested in the configuration.
     *
     * @param platformContext               the platform context
     * @param threadManager                 the thread manager
     * @param ownKeysAndCerts               private keys and public certificates for this node
     * @param roster                        the current roster
     * @param selfId                        this node's ID
     * @param appVersion                    the version of the app
     * @param swirldStateManager            manages the mutable state
     * @param latestCompleteState           holds the latest signed state that has enough signatures to be verifiable
     * @param loadReconnectState            a method that should be called when a state from reconnect is obtained
     * @param clearAllPipelinesForReconnect this method should be called to clear all pipelines prior to a reconnect
     * @param intakeEventCounter            keeps track of the number of events in the intake pipeline from each peer
     * @param platformStateFacade           the facade to access the platform state
     * @param stateRootFunction             a function to instantiate the state root object from a Virtual Map
     */
    public SyncGossipModular(
            @NonNull final PlatformContext platformContext,
            @NonNull final ThreadManager threadManager,
            @NonNull final KeysAndCerts ownKeysAndCerts,
            @NonNull final Roster roster,
            @NonNull final NodeId selfId,
            @NonNull final SemanticVersion appVersion,
            @NonNull final SwirldStateManager swirldStateManager,
            @NonNull final Supplier<ReservedSignedState> latestCompleteState,
            @NonNull final Consumer<SignedState> loadReconnectState,
            @NonNull final Runnable clearAllPipelinesForReconnect,
            @NonNull final IntakeEventCounter intakeEventCounter,
            @NonNull final PlatformStateFacade platformStateFacade,
            @NonNull final Function<VirtualMap, MerkleNodeState> stateRootFunction,
            @NonNull final PlatfromReconnecter platfromReconnecter) {

        final RosterEntry selfEntry = RosterUtils.getRosterEntry(roster, selfId.id());
        final X509Certificate selfCert = RosterUtils.fetchGossipCaCertificate(selfEntry);
        final List<PeerInfo> peers;
        if (!CryptoUtils.checkCertificate(selfCert)) {
            // Do not make peer connections if the self node does not have a valid signing certificate in the roster.
            // https://github.com/hashgraph/hedera-services/issues/16648
            logger.error(
                    EXCEPTION.getMarker(),
                    "The gossip certificate for node {} is missing or invalid. "
                            + "This node will not connect to any peers.",
                    selfId);
            peers = Collections.emptyList();
        } else {
            peers = Utilities.createPeerInfoList(roster, selfId);
        }
        final PeerInfo selfPeer = Utilities.toPeerInfo(selfEntry);

        this.network = new PeerCommunication(platformContext, peers, selfPeer, ownKeysAndCerts);

        final ProtocolConfig protocolConfig = platformContext.getConfiguration().getConfigData(ProtocolConfig.class);

        final int rosterSize = peers.size() + 1;
        final SyncMetrics syncMetrics = new SyncMetrics(platformContext.getMetrics(), platformContext.getTime());

        if (protocolConfig.rpcGossip()) {

            final RpcShadowgraphSynchronizer rpcSynchronizer = new RpcShadowgraphSynchronizer(
                    platformContext,
                    rosterSize,
                    syncMetrics,
                    event -> receivedEventHandler.accept(event),
                    null,
                    intakeEventCounter,
                    selfId);

            this.synchronizer = rpcSynchronizer;

            this.syncProtocol = RpcProtocol.create(
                    platformContext,
                    rpcSynchronizer,
                    intakeEventCounter,
                    threadManager,
                    rosterSize,
                    this.network.getNetworkMetrics(),
                    syncMetrics);

        } else {
            final Shadowgraph shadowgraph = new Shadowgraph(platformContext, rosterSize, intakeEventCounter);

            final ShadowgraphSynchronizer shadowgraphSynchronizer = new ShadowgraphSynchronizer(
                    platformContext,
                    shadowgraph,
                    rosterSize,
                    syncMetrics,
                    event -> receivedEventHandler.accept(event),
                    platfromReconnecter,
                    intakeEventCounter,
                    new CachedPoolParallelExecutor(threadManager, "node-sync"));

            this.synchronizer = shadowgraphSynchronizer;

            this.syncProtocol = SyncProtocol.create(
                    platformContext, shadowgraphSynchronizer, null, intakeEventCounter, rosterSize, syncMetrics);
        }

        reconnectProtocol = createReconnectProtocol(
                platformContext,
                threadManager,
                latestCompleteState,
                roster,
                swirldStateManager,
                selfId,
                platformStateFacade,
                stateRootFunction);
        this.protocols = ImmutableList.of(
                HeartbeatProtocol.create(platformContext, this.network.getNetworkMetrics()),
                reconnectProtocol,
                syncProtocol);

        final VersionCompareHandshake versionCompareHandshake =
                new VersionCompareHandshake(appVersion, !protocolConfig.tolerateMismatchedVersion());
        final List<ProtocolRunnable> handshakeProtocols = List.of(versionCompareHandshake);

        network.initialize(threadManager, handshakeProtocols, protocols);
    }

    /**
     * Utility method for creating ReconnectProtocol from shared state, while staying compatible with pre-refactor code
     *
     * @param platformContext               the platform context
     * @param threadManager                 the thread manager
     * @param latestCompleteState           holds the latest signed state that has enough signatures to be verifiable
     * @param roster                        the current roster
     * @param swirldStateManager            manages the mutable state
     * @param selfId                        this node's ID
     * @param platformStateFacade           the facade to access the platform state
     * @param stateRootFunction             a function to instantiate the state root object from a Virtual Map
     * @return constructed ReconnectProtocol
     */
    public ReconnectProtocol createReconnectProtocol(
            @NonNull final PlatformContext platformContext,
            @NonNull final ThreadManager threadManager,
            @NonNull final Supplier<ReservedSignedState> latestCompleteState,
            @NonNull final Roster roster,
            @NonNull final SwirldStateManager swirldStateManager,
            @NonNull final NodeId selfId,
            @NonNull final PlatformStateFacade platformStateFacade,
            @NonNull final Function<VirtualMap, MerkleNodeState> stateRootFunction) {

        final ReconnectConfig reconnectConfig =
                platformContext.getConfiguration().getConfigData(ReconnectConfig.class);

        final ReconnectThrottle reconnectThrottle = new ReconnectThrottle(reconnectConfig, platformContext.getTime());

        final ReconnectMetrics reconnectMetrics = new ReconnectMetrics(platformContext.getMetrics());

        final StateConfig stateConfig = platformContext.getConfiguration().getConfigData(StateConfig.class);

        final LongSupplier getRoundSupplier = () -> {
            try (final ReservedSignedState reservedState = latestCompleteState.get()) {
                if (reservedState == null || reservedState.isNull()) {
                    return ROUND_UNDEFINED;
                }

                return reservedState.get().getRound();
            }
        };

        final ReconnectLearnerThrottle throttle =
                new ReconnectLearnerThrottle(platformContext.getTime(), selfId, reconnectConfig);

        final ReconnectSyncHelper reconnectNetworkHelper = new ReconnectSyncHelper(
                swirldStateManager::getConsensusState,
                getRoundSupplier,
                new ReconnectLearnerFactory(
                        platformContext,
                        threadManager,
                        roster,
                        reconnectConfig.asyncStreamTimeout(),
                        reconnectMetrics,
                        platformStateFacade,
                        stateRootFunction),
                stateConfig,
                platformStateFacade);

        return new ReconnectProtocol(
                platformContext,
                threadManager,
                reconnectThrottle,
                latestCompleteState,
                reconnectConfig.asyncStreamTimeout(),
                reconnectMetrics,
                reconnectNetworkHelper,
                platformStateFacade);
    }

    /**
     * Modify list of current connected peers. Notify all underlying components and start needed threads. In the case
     * data for the same peer changes (one with the same nodeId), it should be present in both removed and added lists,
     * with old data in removed and fresh data in added. Internally it will be first removed and then added, so there
     * can be a short moment when it will drop out of the network if disconnect happens at a bad moment. NOT THREAD
     * SAFE. Synchronize externally.
     *
     * @param added   peers to be added
     * @param removed peers to be removed
     */
    public void addRemovePeers(@NonNull final List<PeerInfo> added, @NonNull final List<PeerInfo> removed) {
        synchronized (this) {
            syncProtocol.adjustTotalPermits(added.size() - removed.size());
            network.addRemovePeers(added, removed);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void bind(
            @NonNull final WiringModel model,
            @NonNull final BindableInputWire<PlatformEvent, Void> eventInput,
            @NonNull final BindableInputWire<EventWindow, Void> eventWindowInput,
            @NonNull final StandardOutputWire<PlatformEvent> eventOutput,
            @NonNull final BindableInputWire<NoInput, Void> startInput,
            @NonNull final BindableInputWire<NoInput, Void> stopInput,
            @NonNull final BindableInputWire<NoInput, Void> clearInput,
            @NonNull final BindableInputWire<Duration, Void> systemHealthInput,
            @NonNull final BindableInputWire<PlatformStatus, Void> platformStatusInput) {

        startInput.bindConsumer(ignored -> {
            syncProtocol.start();
            network.start();
        });
        stopInput.bindConsumer(ignored -> {
            syncProtocol.stop();
            network.stop();
        });

        clearInput.bindConsumer(ignored -> syncProtocol.clear());
        eventInput.bindConsumer(synchronizer::addEvent);
        eventWindowInput.bindConsumer(synchronizer::updateEventWindow);

        systemHealthInput.bindConsumer(syncProtocol::reportUnhealthyDuration);
        platformStatusInput.bindConsumer(status -> {
            protocols.forEach(protocol -> protocol.updatePlatformStatus(status));
        });

        this.receivedEventHandler = eventOutput::forward;
    }

    @Override
    public ReservedSignedState doReconnect(final MerkleNodeState currentState) {
        return reconnectProtocol.doReconnect(currentState);
    }
}
