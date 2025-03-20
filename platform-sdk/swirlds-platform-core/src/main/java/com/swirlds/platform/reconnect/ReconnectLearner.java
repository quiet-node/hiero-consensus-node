// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.reconnect;

import static com.swirlds.common.formatting.StringFormattingUtils.formattedList;
import static com.swirlds.logging.legacy.LogMarker.EXCEPTION;
import static com.swirlds.logging.legacy.LogMarker.RECONNECT;

import com.hedera.hapi.node.state.roster.Roster;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.io.streams.MerkleDataInputStream;
import com.swirlds.common.io.streams.MerkleDataOutputStream;
import com.swirlds.common.merkle.MerkleNode;
import com.swirlds.common.merkle.synchronization.LearningSynchronizer;
import com.swirlds.common.merkle.synchronization.config.ReconnectConfig;
import com.swirlds.common.threading.manager.ThreadManager;
import com.swirlds.logging.legacy.payload.ReconnectDataUsagePayload;
import com.swirlds.platform.crypto.CryptoStatic;
import com.swirlds.platform.metrics.ReconnectMetrics;
import com.swirlds.platform.network.Connection;
import com.swirlds.platform.roster.RosterUtils;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.signed.ReservedSignedState;
import com.swirlds.platform.state.signed.SigSet;
import com.swirlds.platform.state.signed.SignedState;
import com.swirlds.platform.state.signed.SignedStateInvalidException;
import com.swirlds.platform.state.signed.SignedStateValidationData;
import com.swirlds.platform.state.signed.SignedStateValidator;
import com.swirlds.platform.state.snapshot.SignedStateFileReader;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class encapsulates reconnect logic for the out of date node which is
 * requesting a recent state from another node.
 */
public class ReconnectLearner {

    /** use this for all logging, as controlled by the optional data/log4j2.xml file */
    private static final Logger logger = LogManager.getLogger(ReconnectLearner.class);

    private final Connection connection;
    private final Roster roster;
    private final MerkleNodeState currentState;
    private final Duration reconnectSocketTimeout;
    private final ReconnectMetrics statistics;
    private final SignedStateValidationData stateValidationData;
    private final PlatformStateFacade platformStateFacade;
    private final Function<VirtualMap, MerkleNodeState> stateRootFunction;

    private SigSet sigSet;
    private final PlatformContext platformContext;
    /**
     * After reconnect is finished, restore the socket timeout to the original value.
     */
    private int originalSocketTimeout;

    private final ThreadManager threadManager;

    /**
     * @param threadManager
     * 		responsible for managing thread lifecycles
     * @param connection
     * 		the connection to use for the reconnect
     * @param roster
     * 		the current roster
     * @param currentState
     * 		the most recent state from the learner
     * @param reconnectSocketTimeout
     * 		the amount of time that should be used for the socket timeout
     * @param statistics
     * 		reconnect metrics
     * @param platformStateFacade
     *      the facade to access the platform state
     */
    public ReconnectLearner(
            @NonNull final PlatformContext platformContext,
            @NonNull final ThreadManager threadManager,
            @NonNull final Connection connection,
            @NonNull final Roster roster,
            @NonNull final MerkleNodeState currentState,
            @NonNull final Duration reconnectSocketTimeout,
            @NonNull final ReconnectMetrics statistics,
            @NonNull final PlatformStateFacade platformStateFacade,
            // TODO: add javadoc
            @NonNull Function<VirtualMap, MerkleNodeState> stateRootFunction) {
        this.platformStateFacade = Objects.requireNonNull(platformStateFacade);
        this.stateRootFunction = Objects.requireNonNull(stateRootFunction);

        currentState.throwIfImmutable("Can not perform reconnect with immutable state");
        currentState.throwIfDestroyed("Can not perform reconnect with destroyed state");

        this.platformContext = Objects.requireNonNull(platformContext);
        this.threadManager = Objects.requireNonNull(threadManager);
        this.connection = Objects.requireNonNull(connection);
        this.roster = Objects.requireNonNull(roster);
        this.currentState = Objects.requireNonNull(currentState);
        this.reconnectSocketTimeout = Objects.requireNonNull(reconnectSocketTimeout);
        this.statistics = Objects.requireNonNull(statistics);

        // Save some of the current state data for validation
        this.stateValidationData = new SignedStateValidationData(currentState, roster, platformStateFacade);
    }

    /**
     * @throws ReconnectException
     * 		thrown when there is an error in the underlying protocol
     */
    private void increaseSocketTimeout() throws ReconnectException {
        try {
            originalSocketTimeout = connection.getTimeout();
            connection.setTimeout(reconnectSocketTimeout.toMillis());
        } catch (final SocketException e) {
            throw new ReconnectException(e);
        }
    }

    /**
     * @throws ReconnectException
     * 		thrown when there is an error in the underlying protocol
     */
    private void resetSocketTimeout() throws ReconnectException {
        if (!connection.connected()) {
            logger.debug(
                    RECONNECT.getMarker(),
                    "{} connection to {} is no longer connected. Returning.",
                    connection.getSelfId(),
                    connection.getOtherId());
            return;
        }

        try {
            connection.setTimeout(originalSocketTimeout);
        } catch (final SocketException e) {
            throw new ReconnectException(e);
        }
    }

    /**
     * Perform the reconnect operation.
     *
     * @throws ReconnectException
     * 		thrown if I/O related errors occur, when there is an error in the underlying protocol, or the received
     * 		state is invalid
     * @return the state received from the other node
     */
    @NonNull
    public ReservedSignedState execute(@NonNull final SignedStateValidator validator) throws ReconnectException {
        increaseSocketTimeout();
        ReservedSignedState reservedSignedState = null;
        try {
            receiveSignatures();
            reservedSignedState = reconnect();

            final var signedState = reservedSignedState.get();
            logger.error(
                    EXCEPTION.getMarker(),
                    "(SHOULD BE RESERVED AND SIGNED) isVerifiable={}, sigSet#size={}, signingWeight={}, total weight={}, roster={}",
                    signedState.isVerifiable(),
                    signedState.getSigSet().size(),
                    signedState.getSigningWeight(),
                    RosterUtils.computeTotalWeight(roster),
                    roster);

            try {
                signedState.pruneInvalidSignatures(roster);
                logger.error(
                        EXCEPTION.getMarker(),
                        "(PRUNED INVALID SIGNATURES) isVerifiable={}, sigSet#size={}, signingWeight={}, total weight={}, roster={}",
                        signedState.isVerifiable(),
                        signedState.getSigSet().size(),
                        signedState.getSigningWeight(),
                        RosterUtils.computeTotalWeight(roster),
                        roster);
                signedState.throwIfNotVerifiable();
            } catch (final Exception e) {
                logger.error(EXCEPTION.getMarker(), "STATE IS NOT VERIFIABLE");
            }

            validator.validate(reservedSignedState.get(), roster, stateValidationData);
            ReconnectUtils.endReconnectHandshake(connection);
            return reservedSignedState;
        } catch (final IOException | SignedStateInvalidException e) {
            if (reservedSignedState != null) {
                // if the state was received, we need to release it or it will be leaked
                reservedSignedState.close();
            }
            throw new ReconnectException(e);
        } catch (final InterruptedException e) {
            // an interrupt can only occur in the reconnect() method, so we don't need to close the reservedSignedState
            Thread.currentThread().interrupt();
            throw new ReconnectException("interrupted while attempting to reconnect", e);
        } finally {
            resetSocketTimeout();
        }
    }

    /**
     * Get a copy of the state from the other node.
     *
     * @throws InterruptedException
     * 		if the current thread is interrupted
     */
    @NonNull
    private ReservedSignedState reconnect() throws InterruptedException {
        statistics.incrementReceiverStartTimes();

        final MerkleDataInputStream in = new MerkleDataInputStream(connection.getDis());
        final MerkleDataOutputStream out = new MerkleDataOutputStream(connection.getDos());

        connection.getDis().getSyncByteCounter().resetCount();
        connection.getDos().getSyncByteCounter().resetCount();

        final ReconnectConfig reconnectConfig =
                platformContext.getConfiguration().getConfigData(ReconnectConfig.class);
        final LearningSynchronizer synchronizer = new LearningSynchronizer(
                threadManager,
                in,
                out,
                currentState.getRoot(),
                connection::disconnect,
                platformContext.getMerkleCryptography(),
                reconnectConfig,
                platformContext.getMetrics());
        synchronizer.synchronize();

        final MerkleNode stateRoot = synchronizer.getRoot();
        MerkleNodeState merkleNodeState;

        // TODO: add comments
        // move to stateUtils ?
        if (stateRoot instanceof VirtualMap virtualMap) {
            merkleNodeState = stateRootFunction.apply(virtualMap);
        } else {
            merkleNodeState = (MerkleNodeState) stateRoot;
        }

        final SignedState newSignedState = new SignedState(
                platformContext.getConfiguration(),
                CryptoStatic::verifySignature,
                merkleNodeState,
                "ReconnectLearner.reconnect()",
                false,
                false,
                false,
                platformStateFacade);

        logger.info(
                RECONNECT.getMarker(),
                """
                        The following state received from learner:
                        {}""",
                () -> platformStateFacade.getInfoString(newSignedState.getState(), 5));

        newSignedState.init(platformContext);
        SignedStateFileReader.registerServiceStates(newSignedState);

        logger.info(
                RECONNECT.getMarker(),
                "Received platform state: {}",
                platformStateFacade.platformStateOf(merkleNodeState));

        newSignedState.setSigSet(sigSet);

        final double mbReceived = connection.getDis().getSyncByteCounter().getMebiBytes();
        logger.info(
                RECONNECT.getMarker(),
                () -> new ReconnectDataUsagePayload("Reconnect data usage report", mbReceived).toString());

        statistics.incrementReceiverEndTimes();

        return newSignedState.reserve("ReconnectLearner.reconnect()");
    }

    /**
     * Copy the signatures for the state from the other node.
     *
     * @throws IOException
     * 		if any I/O related errors occur
     */
    private void receiveSignatures() throws IOException {
        logger.info(RECONNECT.getMarker(), "Receiving signed state signatures");
        sigSet = connection.getDis().readSerializable();

        final StringBuilder sb = new StringBuilder();
        sb.append("Received signatures from nodes ");
        formattedList(sb, sigSet.iterator());
        logger.info(RECONNECT.getMarker(), sb);
    }
}
