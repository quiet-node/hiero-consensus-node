// SPDX-License-Identifier: Apache-2.0
package org.hiero.consensus.otter.docker.app.platform;

import static com.swirlds.common.threading.manager.AdHocThreadManager.getStaticThreadManager;
import static com.swirlds.platform.builder.internal.StaticPlatformBuilder.getMetricsProvider;
import static com.swirlds.platform.builder.internal.StaticPlatformBuilder.initLogging;
import static com.swirlds.platform.builder.internal.StaticPlatformBuilder.setupGlobalMetrics;
import static com.swirlds.platform.state.signed.StartupStateUtils.loadInitialState;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.platform.state.NodeId;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.io.filesystem.FileSystemManager;
import com.swirlds.common.io.utility.RecycleBin;
import com.swirlds.common.merkle.crypto.MerkleCryptography;
import com.swirlds.common.merkle.crypto.MerkleCryptographyFactory;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.extensions.test.fixtures.TestConfigBuilder;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.builder.PlatformBuilder;
import com.swirlds.platform.builder.PlatformBuildingBlocks;
import com.swirlds.platform.builder.PlatformComponentBuilder;
import com.swirlds.platform.listeners.PlatformStatusChangeListener;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.signed.HashedReservedSignedState;
import com.swirlds.platform.state.signed.ReservedSignedState;
import com.swirlds.platform.system.Platform;
import com.swirlds.platform.test.fixtures.state.TestingAppStateInitializer;
import com.swirlds.platform.util.BootstrapUtils;
import com.swirlds.platform.wiring.PlatformWiring;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.consensus.model.hashgraph.ConsensusRound;
import org.hiero.consensus.model.node.KeysAndCerts;
import org.hiero.consensus.model.status.PlatformStatus;
import org.hiero.consensus.roster.RosterHistory;
import org.hiero.consensus.roster.RosterUtils;
import org.hiero.otter.fixtures.TransactionFactory;
import org.hiero.otter.fixtures.app.OtterApp;
import org.hiero.otter.fixtures.app.OtterAppState;

/**
 * Manages the lifecycle and operations of a consensus node within a container-based network.
 * This class initializes the platform, handles configuration, and provides methods for interacting
 * with the consensus process, including submitting transactions and listening for consensus rounds.
 */
public class ConsensusNodeManager {
    private static final Logger LOGGER = LogManager.getLogger(ConsensusNodeManager.class);

    private static final String APP_NAME = "org.hiero.consensus.otter.docker.app.platform.DockerApp";
    private static final String SWIRLD_NAME = "123";

    private final Platform platform;
    private final AtomicReference<PlatformStatus> status = new AtomicReference<>();
    private final List<ConsensusRoundListener> consensusRoundListeners = new CopyOnWriteArrayList<>();

    /**
     * Creates a new instance of {@code ConsensusNodeManager} with the specified parameters.
     * This constructor initializes the platform, sets up all necessary parts for the consensus node.
     *
     * @param selfId               the unique identifier for this node, must not be {@code null}
     * @param version              the semantic version of the application, must not be {@code null}
     * @param genesisRoster        the initial roster of nodes in the network, must not be {@code null}
     * @param keysAndCerts         the keys and certificates for this node, must not be {@code null}
     * @param overriddenProperties optional properties to override in the configuration, may be {@code null}
     */
    public ConsensusNodeManager(
            @NonNull final NodeId selfId,
            @NonNull final SemanticVersion version,
            @NonNull final Roster genesisRoster,
            @NonNull final KeysAndCerts keysAndCerts,
            @Nullable final Map<String, String> overriddenProperties) {
        initLogging();
        BootstrapUtils.setupConstructableRegistry();
        TestingAppStateInitializer.registerMerkleStateRootClassIds();

        final var legacySelfId = org.hiero.consensus.model.node.NodeId.of(selfId.id());
        final TestConfigBuilder configurationBuilder = new TestConfigBuilder();
        if (overriddenProperties != null) {
            overriddenProperties.forEach(configurationBuilder::withValue);
        }
        final Configuration platformConfig = configurationBuilder.getOrCreateConfig();

        // Immediately initialize the cryptography and merkle cryptography factories
        // to avoid using default behavior instead of that defined in platformConfig
        final MerkleCryptography merkleCryptography = MerkleCryptographyFactory.create(platformConfig);

        setupGlobalMetrics(platformConfig);
        final Metrics metrics = getMetricsProvider().createPlatformMetrics(legacySelfId);
        final PlatformStateFacade platformStateFacade = new PlatformStateFacade();

        LOGGER.info("Starting node {} with version {}", selfId, version);

        final Time time = Time.getCurrent();
        final FileSystemManager fileSystemManager = FileSystemManager.create(platformConfig);
        final RecycleBin recycleBin = RecycleBin.create(
                metrics, platformConfig, getStaticThreadManager(), time, fileSystemManager, legacySelfId);

        final PlatformContext platformContext = PlatformContext.create(
                platformConfig, Time.getCurrent(), metrics, fileSystemManager, recycleBin, merkleCryptography);

        final HashedReservedSignedState reservedState = loadInitialState(
                recycleBin,
                version,
                () -> OtterAppState.createGenesisState(platformConfig, genesisRoster, version),
                APP_NAME,
                SWIRLD_NAME,
                legacySelfId,
                platformStateFacade,
                platformContext);
        final ReservedSignedState initialState = reservedState.state();

        final MerkleNodeState state = initialState.get().getState();
        final RosterHistory rosterHistory = RosterUtils.createRosterHistory(state);

        final PlatformBuilder builder = PlatformBuilder.create(
                        APP_NAME,
                        SWIRLD_NAME,
                        version,
                        initialState,
                        OtterApp.INSTANCE,
                        legacySelfId,
                        selfId.toString(),
                        rosterHistory,
                        platformStateFacade)
                .withPlatformContext(platformContext)
                .withConfiguration(platformConfig)
                .withKeysAndCerts(keysAndCerts)
                .withSystemTransactionEncoderCallback(txn -> Bytes.wrap(
                        TransactionFactory.createStateSignatureTransaction(txn).toByteArray()));

        // Build the platform component builder
        final PlatformComponentBuilder componentBuilder = builder.buildComponentBuilder();
        final PlatformBuildingBlocks blocks = componentBuilder.getBuildingBlocks();

        // Wiring: Forward consensus rounds to registered listeners
        final PlatformWiring wiring = blocks.platformWiring();
        wiring.getConsensusEngineOutputWire()
                .solderTo("dockerApp", "consensusRounds", this::notifyConsensusRoundListeners);

        platform = componentBuilder.build();

        platform.getNotificationEngine()
                .register(PlatformStatusChangeListener.class, newStatus -> status.set(newStatus.getNewStatus()));
    }

    /**
     * Starts the consensus node. This method starts the consensus node platform and application so that it can start receiving transactions.
     */
    public void start() {
        platform.start();
    }

    /**
     * Shuts down the consensus node. Once destroyed, it cannot be restarted.
     *
     * @throws InterruptedException if the thread is interrupted while waiting for the platform to shut down
     */
    public void destroy() throws InterruptedException {
        platform.destroy();
    }

    /**
     * Registers a listener to receive notifications about changes in the platform's status.
     *
     * @param listener the listener to register, must not be {@code null}
     */
    public void registerPlatformStatusChangeListener(@NonNull final PlatformStatusChangeListener listener) {
        platform.getNotificationEngine().register(PlatformStatusChangeListener.class, listener);
    }

    /**
     * Notifies registered listeners about new consensus rounds.
     *
     * @param rounds the list of consensus rounds to notify listeners about, must not be {@code null}
     */
    private void notifyConsensusRoundListeners(@NonNull final List<ConsensusRound> rounds) {
        consensusRoundListeners.forEach(listener -> listener.onConsensusRounds(rounds));
    }

    /**
     * Submits a raw transaction to the underlying platform for processing.
     *
     * @param transaction the serialized transaction bytes, must not be {@code null}
     * @return {@code true} if the transaction was successfully submitted, {@code false} otherwise
     */
    public boolean submitTransaction(@NonNull final byte[] transaction) {
        return platform.createTransaction(transaction);
    }

    /**
     * Registers a listener to receive notifications about new consensus rounds.
     *
     * @param listener the listener to register, must not be {@code null}
     */
    public void registerConsensusRoundListener(@NonNull final ConsensusRoundListener listener) {
        consensusRoundListeners.add(listener);
    }
}
