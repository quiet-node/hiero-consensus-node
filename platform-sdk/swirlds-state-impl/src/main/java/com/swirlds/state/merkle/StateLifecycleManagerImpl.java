// SPDX-License-Identifier: Apache-2.0
package com.swirlds.state.merkle;

import static com.swirlds.base.units.UnitConstants.NANOSECONDS_TO_MICROSECONDS;
import static java.util.Objects.requireNonNull;

import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.State;
import com.swirlds.state.lifecycle.StateLifecycleManager;
import com.swirlds.state.lifecycle.StateLifecycleMetrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicReference;

public class StateLifecycleManagerImpl<T extends MerkleStateRoot<T>> implements StateLifecycleManager<T> {
    /**
     * reference to the state that reflects all known consensus transactions
     */
    private final AtomicReference<T> stateRef = new AtomicReference<>();

    /**
     * The most recent immutable state. No value until the first fast copy is created.
     */
    private final AtomicReference<T> latestImmutableState = new AtomicReference<>();

    private final StateLifecycleMetrics stateLifecycleMetrics;

    public StateLifecycleManagerImpl(Metrics metrics) {
        this.stateLifecycleMetrics = new StateLifecycleMetrics(metrics);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInitialState(@NonNull T state) {
        setInitialState(state, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void overwriteExistingState(@NonNull T state) {
        setInitialState(state, true);
    }

    private void setInitialState(@NonNull T state, boolean overwrite) {
        requireNonNull(state);

        if (!overwrite && stateRef.get() != null) {
            throw new IllegalStateException("Attempt to set initial state when there is already a state reference.");
        }

        state.throwIfDestroyed("state must not be destroyed");
        state.throwIfImmutable("state must be mutable");

        // Create a fast copy so there is always an immutable state to
        // invoke handleTransaction on for pre-consensus transactions
        fastCopyAndUpdateRefs(state);
    }

    private void fastCopyAndUpdateRefs(final T state) {
        final long copyStart = System.nanoTime();

        // Create a fast copy
        final T copy = state.copy();
        // Increment the reference count because this reference becomes the new value
        copy.reserve();

        final long copyEnd = System.nanoTime();

        stateLifecycleMetrics.stateCopyMicros((copyEnd - copyStart) * NANOSECONDS_TO_MICROSECONDS);

        // Set latest immutable first to prevent the newly immutable stateRoot from being deleted between setting the
        // stateRef and the latestImmutableState
        setLatestImmutableState(state);
        setState(copy);
    }

    private void setLatestImmutableState(final T immutableState) {
        final State currVal = latestImmutableState.get();
        if (currVal != null) {
            currVal.release();
        }
        immutableState.reserve();
        latestImmutableState.set(immutableState);
    }

    /**
     * Sets the consensus state to the state provided. Must be mutable and have a reference count of at least 1.
     *
     * @param state a new mutable state
     */
    private void setState(final T state) {
        final var currVal = stateRef.get();
        if (currVal != null) {
            currVal.release();
        }
        // Do not increment the reference count because the state provided already has a reference count of at least
        // one to represent this reference and to prevent it from being deleted before this reference is set.
        stateRef.set(state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getMutableState() {
        return stateRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T copyMutableState() {
        fastCopyAndUpdateRefs(stateRef.get());
        return latestImmutableState.get();
    }
}
