// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.container;

import static java.util.Objects.requireNonNull;
import static org.hiero.otter.fixtures.container.ContainerImage.CONTROL_PORT;
import static org.hiero.otter.fixtures.internal.AbstractNode.LifeCycle.DESTROYED;
import static org.hiero.otter.fixtures.internal.AbstractNode.LifeCycle.INIT;
import static org.hiero.otter.fixtures.internal.AbstractNode.LifeCycle.RUNNING;
import static org.hiero.otter.fixtures.internal.AbstractNode.LifeCycle.SHUTDOWN;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ByteString;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.platform.state.NodeId;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.consensus.model.node.KeysAndCerts;
import org.hiero.consensus.model.status.PlatformStatus;
import org.hiero.otter.fixtures.AsyncNodeActions;
import org.hiero.otter.fixtures.KeysAndCertsConverter;
import org.hiero.otter.fixtures.Node;
import org.hiero.otter.fixtures.NodeConfiguration;
import org.hiero.otter.fixtures.ProtobufConverter;
import org.hiero.otter.fixtures.container.proto.EventMessage;
import org.hiero.otter.fixtures.container.proto.KillImmediatelyRequest;
import org.hiero.otter.fixtures.container.proto.PlatformStatusChange;
import org.hiero.otter.fixtures.container.proto.StartRequest;
import org.hiero.otter.fixtures.container.proto.TestControlGrpc;
import org.hiero.otter.fixtures.container.proto.TestControlGrpc.TestControlStub;
import org.hiero.otter.fixtures.container.proto.TransactionRequest;
import org.hiero.otter.fixtures.container.proto.TransactionRequestAnswer;
import org.hiero.otter.fixtures.internal.AbstractNode;
import org.hiero.otter.fixtures.internal.result.NodeResultsCollector;
import org.hiero.otter.fixtures.internal.result.SingleNodeLogResultImpl;
import org.hiero.otter.fixtures.logging.StructuredLog;
import org.hiero.otter.fixtures.result.SingleNodeConsensusResult;
import org.hiero.otter.fixtures.result.SingleNodeLogResult;
import org.hiero.otter.fixtures.result.SingleNodePcesResult;
import org.hiero.otter.fixtures.result.SingleNodePlatformStatusResults;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Implementation of {@link Node} for a container environment.
 */
public class ContainerNode extends AbstractNode implements Node {

    private static final Logger log = LogManager.getLogger();

    public static final int GOSSIP_PORT = 5777;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(1);

    private final ContainerImage container;
    private final Roster roster;
    private final KeysAndCerts keysAndCerts;
    private final ManagedChannel channel;
    private final TestControlGrpc.TestControlBlockingStub blockingStub;
    private final AsyncNodeActions defaultAsyncAction = withTimeout(DEFAULT_TIMEOUT);
    private final ContainerNodeConfiguration nodeConfiguration = new ContainerNodeConfiguration();
    private final NodeResultsCollector resultsCollector;
    private final List<StructuredLog> receivedLogs = new CopyOnWriteArrayList<>();

    /**
     * Constructor for the {@link ContainerNode} class.
     *
     * @param selfId       the unique identifier for this node
     * @param roster       the roster of the network
     * @param keysAndCerts the keys for the node
     * @param network      the network this node is part of
     * @param dockerImage  the Docker image to use for this node
     */
    public ContainerNode(
            @NonNull final NodeId selfId,
            @NonNull final Roster roster,
            @NonNull final KeysAndCerts keysAndCerts,
            @NonNull final Network network,
            @NonNull final ImageFromDockerfile dockerImage) {
        super(selfId);
        this.roster = requireNonNull(roster, "roster must not be null");
        this.keysAndCerts = requireNonNull(keysAndCerts, "keysAndCerts must not be null");

        this.resultsCollector = new NodeResultsCollector(selfId);

        //noinspection resource
        container = new ContainerImage(dockerImage, network, selfId);
        container.start();
        channel = ManagedChannelBuilder.forAddress(container.getHost(), container.getMappedPort(CONTROL_PORT))
                .maxInboundMessageSize(32 * 1024 * 1024)
                .usePlaintext()
                .build();

        blockingStub = TestControlGrpc.newBlockingStub(channel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killImmediately() throws InterruptedException {
        defaultAsyncAction.killImmediately();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        defaultAsyncAction.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AsyncNodeActions withTimeout(@NonNull final Duration timeout) {
        return new ContainerAsyncNodeActions(timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void submitTransaction(@NonNull final byte[] transaction) {
        throwIfIn(INIT, "Node has not been started yet.");
        throwIfIn(SHUTDOWN, "Node has been shut down.");
        throwIfIn(DESTROYED, "Node has been destroyed.");

        try {
            final TransactionRequest request = TransactionRequest.newBuilder()
                    .setPayload(ByteString.copyFrom(transaction))
                    .build();

            final TransactionRequestAnswer answer = blockingStub.submitTransaction(request);
            if (!answer.getResult()) {
                fail("Failed to submit transaction for node %d.".formatted(selfId.id()));
            }
        } catch (final Exception e) {
            fail("Failed to submit transaction to node %d".formatted(selfId.id()), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public NodeConfiguration<?> configuration() {
        return nodeConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public SingleNodeConsensusResult getConsensusResult() {
        return resultsCollector.getConsensusResult();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public SingleNodeLogResult getLogResult() {
        return new SingleNodeLogResultImpl(selfId, Set.of());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public SingleNodePlatformStatusResults getPlatformStatusResults() {
        return resultsCollector.getStatusProgression();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public SingleNodePcesResult getPcesResult() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    /**
     * Shuts down the node and cleans up resources. Once this method is called, the node cannot be started again. This
     * method is idempotent and can be called multiple times without any side effects.
     */
    void destroy() {
        if (lifeCycle == RUNNING) {
            channel.shutdownNow();
            container.stop();
        }
        resultsCollector.destroy();
        platformStatus = null;
        lifeCycle = DESTROYED;
    }

    /**
     * Container-specific implementation of {@link AsyncNodeActions}.
     */
    private class ContainerAsyncNodeActions implements AsyncNodeActions {

        private final Duration timeout;

        /**
         * Constructor for the {@link ContainerAsyncNodeActions} class.
         *
         * @param timeout the duration to wait for actions to complete
         */
        public ContainerAsyncNodeActions(@NonNull final Duration timeout) {
            this.timeout = timeout;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void start() {
            throwIfIn(LifeCycle.RUNNING, "Node has already been started.");
            throwIfIn(LifeCycle.DESTROYED, "Node has already been destroyed.");

            log.info("Starting node {}...", selfId);

            final StartRequest startRequest = StartRequest.newBuilder()
                    .setSelfId(ProtobufConverter.fromPbj(selfId))
                    .setRoster(ProtobufConverter.fromPbj(roster))
                    .setKeysAndCerts(KeysAndCertsConverter.toProto(keysAndCerts))
                    .setVersion(ProtobufConverter.fromPbj(version))
                    .putAllOverriddenProperties(nodeConfiguration.overriddenProperties())
                    .build();

            final TestControlStub stub = TestControlGrpc.newStub(channel);
            stub.start(startRequest, new StreamObserver<>() {
                @Override
                public void onNext(final EventMessage value) {
                    switch (value.getEventCase()) {
                        case PLATFORM_STATUS_CHANGE -> handlePlatformChange(value);
                        case LOG_ENTRY -> receivedLogs.add(ProtobufConverter.toPlatform(value.getLogEntry()));
                        case CONSENSUS_ROUNDS ->
                            resultsCollector.addConsensusRounds(ProtobufConverter.toPbj(value.getConsensusRounds()));
                        default -> {
                            final String message = String.format(
                                    "Received unknown message type from node %s: %s", selfId, value.getEventCase());
                            throw new RuntimeException(message);
                        }
                    }
                }

                @Override
                public void onError(@NonNull final Throwable error) {
                    /*
                     * After a call to killImmediately() the server forcibly closes the stream and the
                     * client receives an INTERNAL error. This is expected and must *not* fail the test.
                     * Only report unexpected errors that occur while the node is still running.
                     */
                    if (lifeCycle == RUNNING) {
                        if (!isExpectedError(error)) {
                            final String message = String.format("gRPC error from node %s", selfId);
                            fail(message, error);
                        }
                    }
                }

                private static boolean isExpectedError(final @NonNull Throwable error) {
                    if (error instanceof StatusRuntimeException sre) {
                        final Code code = sre.getStatus().getCode();
                        return code == Code.UNAVAILABLE || code == Code.CANCELLED || code == Code.INTERNAL;
                    }
                    return false;
                }

                @Override
                public void onCompleted() {
                    if (lifeCycle != DESTROYED && lifeCycle != SHUTDOWN) {
                        fail("Node " + selfId + " has closed the connection while running the test");
                    }
                }
            });

            lifeCycle = RUNNING;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("ResultOfMethodCallIgnored") // ignoring the Empty answer from killImmediately
        public void killImmediately() {
            log.info("Killing node {} immediately...", selfId);
            try {
                // Mark the node as shutting down *before* sending the request to avoid race
                // conditions with the stream observer receiving an error.
                lifeCycle = SHUTDOWN;

                final KillImmediatelyRequest request = KillImmediatelyRequest.getDefaultInstance();
                // Unary call – will throw if server returns an error.
                blockingStub.killImmediately(request);
            } catch (final Exception e) {
                fail("Failed to kill node %d immediately".formatted(selfId.id()), e);
            }
        }
    }

    private void handlePlatformChange(@NonNull final EventMessage value) {
        final PlatformStatusChange change = value.getPlatformStatusChange();
        final String statusName = change.getNewStatus();
        try {
            final PlatformStatus newStatus = PlatformStatus.valueOf(statusName);
            platformStatus = newStatus;
            resultsCollector.addPlatformStatus(newStatus);
        } catch (final IllegalArgumentException e) {
            log.warn("Received unknown platform status: {}", statusName);
        }
    }
}
