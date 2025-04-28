// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.turtle;

import static com.swirlds.common.threading.manager.AdHocThreadManager.getStaticThreadManager;
import static com.swirlds.platform.builder.internal.StaticPlatformBuilder.getMetricsProvider;
import static com.swirlds.platform.builder.internal.StaticPlatformBuilder.setupGlobalMetrics;
import static com.swirlds.platform.state.signed.ReservedSignedState.createNullReservation;
import static com.swirlds.platform.state.signed.StartupStateUtils.getInitialState;
import static com.swirlds.platform.state.signed.StartupStateUtils.initGenesisState;
import static java.util.Objects.requireNonNull;
import static org.hiero.otter.fixtures.turtle.TurtleTestEnvironment.APP_NAME;
import static org.hiero.otter.fixtures.turtle.TurtleTestEnvironment.SWIRLD_NAME;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.io.config.FileSystemManagerConfig_;
import com.swirlds.common.io.filesystem.FileSystemManager;
import com.swirlds.common.io.utility.FileUtils;
import com.swirlds.common.io.utility.RecycleBin;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.platform.TestPlatformContextBuilder;
import com.swirlds.component.framework.model.DeterministicWiringModel;
import com.swirlds.component.framework.model.WiringModelBuilder;
import com.swirlds.config.api.Configuration;
import com.swirlds.merkledb.MerkleDb;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.builder.PlatformBuilder;
import com.swirlds.platform.builder.PlatformBuildingBlocks;
import com.swirlds.platform.builder.PlatformComponentBuilder;
import com.swirlds.platform.crypto.CryptoStatic;
import com.swirlds.platform.crypto.KeysAndCerts;
import com.swirlds.platform.listeners.PlatformStatusChangeListener;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.PlatformStateModifier;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.signed.HashedReservedSignedState;
import com.swirlds.platform.state.signed.ReservedSignedState;
import com.swirlds.platform.state.signed.SignedState;
import com.swirlds.platform.state.signed.StartupStateUtils;
import com.swirlds.platform.system.Platform;
import com.swirlds.platform.system.address.AddressBookUtils;
import com.swirlds.platform.test.fixtures.turtle.gossip.SimulatedGossip;
import com.swirlds.platform.test.fixtures.turtle.gossip.SimulatedNetwork;
import com.swirlds.platform.test.fixtures.turtle.runner.TurtleTestingToolState;
import com.swirlds.platform.util.RandomBuilder;
import com.swirlds.platform.wiring.PlatformWiring;
import com.swirlds.state.State;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.hiero.consensus.model.node.NodeId;
import org.hiero.consensus.model.roster.AddressBook;
import org.hiero.consensus.model.status.PlatformStatus;
import org.hiero.consensus.roster.RosterUtils;
import org.hiero.otter.fixtures.Node;
import org.hiero.otter.fixtures.NodeConfiguration;
import org.hiero.otter.fixtures.turtle.app.TurtleApp;

/**
 * A node in the turtle network.
 *
 * <p>This class implements the {@link Node} interface and provides methods to control the state of the node.
 */
@SuppressWarnings("removal")
public class TurtleNode implements Node, TurtleTimeManager.TimeTickReceiver {

    private static final SemanticVersion DEFAULT_VERSION =
            SemanticVersion.newBuilder().major(1).build();

    public static final String THREAD_CONTEXT_NODE_ID = "nodeId";
    private static final Logger log = LogManager.getLogger(TurtleNode.class);

    private enum LifeCycle {
        INIT,
        STARTED,
        SHUTDOWN,
        DESTROYED
    }

    private final NodeId selfId;

    private final Randotron randotron;
    private final Time time;
    private final AddressBook addressBook;
    private final KeysAndCerts privateKeys;
    private final SimulatedNetwork network;
    private final TurtleNodeConfiguration nodeConfiguration;

    private final PlatformStatusChangeListener platformStatusChangeListener =
            data -> TurtleNode.this.platformStatus = data.getNewStatus();

    private DeterministicWiringModel model;
    private Platform platform;
    private PlatformWiring platformWiring;
    private LifeCycle lifeCycle = LifeCycle.INIT;

    private PlatformStatus platformStatus;
    private HashedReservedSignedState reservedState;
    private ReservedSignedState initialState;
    private State state;
    private MerkleNodeState rootNode;
    private PlatformComponentBuilder platformComponentBuilder;
    private PlatformStateModifier platformStateModifier;
    private PlatformBuilder platformBuilder;
    private PlatformStateFacade platformStateFacade;
    private TurtleTestingToolState turtleTestingToolState;

    private PlatformBuildingBlocks platformBuildingBlocks;

    public TurtleNode(
            @NonNull final Randotron randotron,
            @NonNull final Time time,
            @NonNull final NodeId selfId,
            @NonNull final AddressBook addressBook,
            @NonNull final KeysAndCerts privateKeys,
            @NonNull final SimulatedNetwork network,
            @NonNull final Path outputDirectory) {
        try {
            ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());

            this.randotron = requireNonNull(randotron);
            this.time = requireNonNull(time);
            this.selfId = requireNonNull(selfId);
            this.addressBook = requireNonNull(addressBook);
            this.privateKeys = requireNonNull(privateKeys);
            this.network = requireNonNull(network);
            this.nodeConfiguration = new TurtleNodeConfiguration(outputDirectory);

        } finally {
            ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
        }
    }

    /**
     * Returns the status of the platform while the node is running or {@code null} if not.
     *
     * @return the status of the platform
     */
    PlatformStatus platformStatus() {
        return platformStatus;
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public NodeId getSelfId() {
        return selfId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void failUnexpectedly(@NonNull final Duration timeout) throws InterruptedException {
        try {
            ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());

            doShutdownNode();

        } finally {
            ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdownGracefully(@NonNull final Duration timeout) throws InterruptedException {
        try {
            ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());

//            platformWiring.flushIntakePipeline();
            doShutdownNode();

        } finally {
            ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void revive(@NonNull final Duration timeout) {
        try {
            ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());

            checkLifeCycle(LifeCycle.STARTED, "Node has already been started.");
            checkLifeCycle(LifeCycle.DESTROYED, "Node has already been destroyed.");

            // Start node from current state
            doStartNode();

        } finally {
            ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void submitTransaction(@NonNull final byte[] transaction) {
        try {
            ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());

            checkLifeCycle(LifeCycle.INIT, "Node has not been started yet.");
            checkLifeCycle(LifeCycle.SHUTDOWN, "Node has been shut down.");
            checkLifeCycle(LifeCycle.DESTROYED, "Node has been destroyed.");

            platform.createTransaction(transaction);

        } finally {
            ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public NodeConfiguration getConfiguration() {
        return nodeConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void tick(@NonNull Instant now) {
        if (lifeCycle == LifeCycle.STARTED) {
            try {
                ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());
                model.tick();
            } finally {
                ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
            }
        }
    }

    /**
     * Start the node
     */
    public void start() {
        System.out.println("Starting node: " + selfId);

        try {
            ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());

            checkLifeCycle(LifeCycle.STARTED, "Node has already been started.");
            checkLifeCycle(LifeCycle.DESTROYED, "Node has already been destroyed.");

            // Clean the output directory and start the node
            final String rootPath =
                    nodeConfiguration.createConfiguration().getValue(FileSystemManagerConfig_.ROOT_PATH);
            log.info("Deleting directory: {}", rootPath);
            if (rootPath != null) {
                try {
                    FileUtils.deleteDirectory(new File(rootPath).toPath());
                } catch (IOException ex) {
                    log.warn("Failed to delete directory: {}", rootPath, ex);
                }
            }
            doStartNode();

        } finally {
            ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
        }
    }

    /**
     * Shuts down the node and cleans up resources. Once this method is called, the node cannot be started again. This
     * method is idempotent and can be called multiple times without any side effects.
     */
    public void destroy() throws InterruptedException {
        try {
            ThreadContext.put(THREAD_CONTEXT_NODE_ID, selfId.toString());

            doShutdownNode();
            lifeCycle = LifeCycle.DESTROYED;

        } finally {
            ThreadContext.remove(THREAD_CONTEXT_NODE_ID);
        }
    }

    private void checkLifeCycle(@NonNull final LifeCycle expected, @NonNull final String message) {
        if (lifeCycle == expected) {
            throw new IllegalStateException(message);
        }
    }

    private void doShutdownNode() throws InterruptedException {
        System.out.println("Shutting down node: " + selfId);

        if (lifeCycle == LifeCycle.STARTED) {
            // TODO: Release all resources
            getMetricsProvider().removePlatformMetrics(platform.getSelfId());
//            platformWiring.stop();
            platform.stop();

//            rootNode.release();
//            platform.getNotificationEngine().unregisterAll();

            platformStatus = null;
            platform = null;
            platformWiring = null;
            model = null;
//            reservedState = null;

            rootNode = null;
//            if (!initialState.is()) {
//            initialState.close();
            initialState.delete();
//            }

            initialState = null;
            state = null;
            platformBuildingBlocks = null;

            TurtleTestingToolState.closeState();
            platformStateModifier.clean();

            platformBuilder.clear();
            platformBuilder = null;
            platformStateFacade = null;

            turtleTestingToolState.destroy();
//            turtleTestingToolState = null;
        }
        lifeCycle = LifeCycle.SHUTDOWN;
    }

    private void doStartNode() {

        final Configuration currentConfiguration = nodeConfiguration.createConfiguration();

        setupGlobalMetrics(currentConfiguration);

        final PlatformContext platformContext = TestPlatformContextBuilder.create()
                .withTime(time)
                .withConfiguration(currentConfiguration)
                .build();

        model = WiringModelBuilder.create(platformContext.getMetrics(), time)
                .withDeterministicModeEnabled(true)
                .build();
        final SemanticVersion version = currentConfiguration.getValue(
                TurtleNodeConfiguration.SOFTWARE_VERSION, SemanticVersion.class, DEFAULT_VERSION);
        platformStateFacade = new PlatformStateFacade();
        MerkleDb.resetDefaultInstancePath();
        final Metrics metrics = getMetricsProvider().createPlatformMetrics(selfId);
        final FileSystemManager fileSystemManager = FileSystemManager.create(currentConfiguration);
        final RecycleBin recycleBin = RecycleBin.create(
                metrics, currentConfiguration, getStaticThreadManager(), time, fileSystemManager, selfId);

        turtleTestingToolState =
                new TurtleTestingToolState();
//        rootNode = TurtleTestingToolState::getStateRootNode;
        rootNode = turtleTestingToolState.getStateRootNode();
//        reservedState = getInitialState(
//                recycleBin,
//                version,
//                rootNode,
//                APP_NAME,
//                SWIRLD_NAME,
//                selfId,
//                addressBook,
//                platformStateFacade,
//                platformContext);
//        initialState = reservedState.state();

        initialState = buildGenesisState(
                addressBook,
                version,
                rootNode,
                platformStateFacade,
                platformContext);

        state = initialState.get().getState();
        final long round = platformStateFacade.roundOf(state);
        platformBuilder = PlatformBuilder.create(
                        APP_NAME,
                        SWIRLD_NAME,
                        version,
                        initialState,
                        TurtleApp.INSTANCE,
                        selfId,
                        AddressBookUtils.formatConsensusEventStreamName(addressBook, selfId),
                        RosterUtils.buildRosterHistory(state, round),
                        platformStateFacade)
                .withModel(model)
                .withRandomBuilder(new RandomBuilder(randotron.nextLong()))
                .withKeysAndCerts(privateKeys)
                .withPlatformContext(platformContext)
                .withConfiguration(currentConfiguration)
                .withSystemTransactionEncoderCallback(txn -> Bytes.wrap(
                        TransactionFactory.createStateSignatureTransaction(txn).toByteArray()));

        platformComponentBuilder = platformBuilder.buildComponentBuilder();
        platformBuildingBlocks = platformComponentBuilder.getBuildingBlocks();

        final SimulatedGossip gossip = network.getGossipInstance(selfId);
        gossip.provideIntakeEventCounter(platformBuildingBlocks.intakeEventCounter());

        platformComponentBuilder.withMetricsDocumentationEnabled(false).withGossip(network.getGossipInstance(selfId));

        platformWiring = platformBuildingBlocks.platformWiring();

        platform = platformComponentBuilder.build();
        platformStatus = PlatformStatus.STARTING_UP;
        platform.getNotificationEngine().register(PlatformStatusChangeListener.class, platformStatusChangeListener);
        platform.start();

        lifeCycle = LifeCycle.STARTED;
    }

    @Override
    public void stop() throws InterruptedException {
//        platform.stop();
        doShutdownNode();
    }

    private ReservedSignedState buildGenesisState(
            @NonNull final AddressBook addressBook,
            @NonNull final SemanticVersion appVersion,
            @NonNull final MerkleNodeState stateRoot,
            @NonNull final PlatformStateFacade platformStateFacade,
            @NonNull final PlatformContext platformContext) {
        platformStateModifier = initGenesisState(platformContext.getConfiguration(), stateRoot, platformStateFacade, addressBook, appVersion);

        final SignedState signedState = new SignedState(
                platformContext.getConfiguration(),
                CryptoStatic::verifySignature,
                stateRoot,
                "genesis state",
                false,
                false,
                false,
                platformStateFacade);
        signedState.init(platformContext);
        return signedState.reserve("initial reservation on genesis state");
    }
}
