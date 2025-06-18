// SPDX-License-Identifier: Apache-2.0
package org.hiero.consensus.roster;

import com.hedera.hapi.node.state.primitives.ProtoBytes;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.node.state.roster.RosterState;
import com.hedera.hapi.node.state.roster.RoundRosterPair;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.List;
import java.util.Optional;

import static com.swirlds.state.BinaryStateUtils.getValidatedStateId;
import static java.util.Objects.requireNonNull;

/**
 * Provides read-only methods for interacting with the underlying data storage mechanisms for
 * working with Rosters.
 */
public class ReadableRosterStoreImpl implements ReadableRosterStore {

    protected final BinaryState binaryState;

    protected final int rosterStateStateId;

    protected final int rosterMapStateId;

    /**
     * Create a new {@link ReadableRosterStore} instance.
     *
     * @param binaryState The state to use.
     */
    public ReadableRosterStoreImpl(@NonNull final BinaryState binaryState) {
        requireNonNull(binaryState);
        this.binaryState = binaryState;
        this.rosterStateStateId = getValidatedStateId(RosterStateId.NAME, WritableRosterStore.ROSTER_STATES_KEY);
        this.rosterMapStateId = getValidatedStateId(RosterStateId.NAME, WritableRosterStore.ROSTER_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public Roster getCandidateRoster() {
        final RosterState rosterState = getRosterState();
        if (rosterState == null) {
            return null;
        }
        final Bytes candidateRosterHash = rosterState.candidateRosterHash();
        return getRosterByHash(candidateRosterHash);
    }

    protected RosterState getRosterState() {
        return binaryState.getSingleton(rosterStateStateId, RosterState.PROTOBUF);
    }

    protected Roster getRosterByHash(Bytes candidateRosterHash) {
        return binaryState.getValueByKey(rosterMapStateId, ProtoBytes.PROTOBUF, ProtoBytes.newBuilder()
                .value(candidateRosterHash)
                .build(), Roster.PROTOBUF);
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public Roster getActiveRoster() {
        final var activeRosterHash = getCurrentRosterHash();
        if (activeRosterHash == null) {
            return null;
        }
        return getRosterByHash(activeRosterHash);
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public Roster get(@NonNull final Bytes rosterHash) {
        return getRosterByHash(rosterHash);
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public Bytes getCurrentRosterHash() {
        final RosterState rosterStateSingleton = getRosterState();
        if (rosterStateSingleton == null) {
            return null;
        }
        final List<RoundRosterPair> rostersAndRounds = rosterStateSingleton.roundRosterPairs();
        if (rostersAndRounds.isEmpty()) {
            return null;
        }
        // by design, the first round roster pair is the active roster
        // this may need to be revisited when we reach DAB
        final RoundRosterPair latestRoundRosterPair = rostersAndRounds.getFirst();
        return latestRoundRosterPair.activeRosterHash();
    }

    @Nullable
    @Override
    public Bytes getPreviousRosterHash() {
        final var rosterHistory = getRosterHistory();
        return rosterHistory.size() > 1 ? rosterHistory.get(1).activeRosterHash() : null;
    }

    /** {@inheritDoc} */
    @Override
    public @NonNull List<RoundRosterPair> getRosterHistory() {
        return requireNonNull(getRosterState()).roundRosterPairs().stream()
                .filter(pair -> getRosterByHash(pair.activeRosterHash()) != null)
                .toList();
    }

    @Override
    public @Nullable Bytes getCandidateRosterHash() {
        return Optional.ofNullable(getRosterState())
                .map(RosterState::candidateRosterHash)
                .filter(bytes -> bytes.length() > 0)
                .orElse(null);
    }
}
