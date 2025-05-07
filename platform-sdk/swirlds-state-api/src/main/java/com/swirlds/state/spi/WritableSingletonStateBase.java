// SPDX-License-Identifier: Apache-2.0
package com.swirlds.state.spi;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;

/**
 * A convenient base class for mutable singletons.
 *
 * @param <T> The type
 */
public abstract class WritableSingletonStateBase<T> extends ReadableSingletonStateBase<T>
        implements WritableSingletonState<T> {

    /**
     * A sentinel value to represent null in the backing store.
     */
    private static final Object NULL_VALUE = new Object();

    /** Modified value buffered in this mutable state */
    private Object value;

    /** A list of listeners to be notified of changes to the state */
    private final List<SingletonChangeListener<T>> listeners = new ArrayList<>();

    /**
     * Creates a new instance.
     *
     * @param serviceName The name of the service that owns the state
     * @param stateKey The state key for this instance
     */
    public WritableSingletonStateBase(@NonNull final String serviceName, @NonNull final String stateKey) {
        super(serviceName, stateKey);
    }

    /**
     * Register a listener to be notified of changes to the state on {@link #commit()}. We do not support unregistering
     * a listener, as the lifecycle of a {@link WritableSingletonState} is scoped to the set of mutations made to a
     * state in a round; and there is no case where an application would only want to be notified of a subset of those
     * changes.
     *
     * @param listener the listener to register
     */
    public void registerListener(@NonNull final SingletonChangeListener<T> listener) {
        requireNonNull(listener);
        listeners.add(listener);
    }

    @Override
    public T get() {
        // Possible pattern: "put" and then "get". In this case, "read" should be false!! Otherwise,
        // we invalidate tx when we don't need to
        final var currentValue = currentValue();
        if (currentValue != null) {
            // C.f. https://github.com/hashgraph/hedera-services/issues/14582; in principle we should
            // also return null here if value is NULL_VALUE, but in production with the SingletonNode
            // backing store, null values are never actually set so this doesn't matter
            return currentValue;
        }
        return super.get();
    }

    @Override
    public void put(T value) {
        this.value = value == null ? NULL_VALUE : value;
    }

    @Override
    public boolean isModified() {
        return value != null;
    }

    /**
     * Flushes all changes into the underlying data store. This method should <strong>ONLY</strong>
     * be called by the code that created the {@link WritableSingletonStateBase} instance or owns
     * it. Don't cast and commit unless you own the instance!
     */
    public void commit() {
        if (isModified()) {
            if (currentValue() != null) {
                putIntoDataSource(currentValue());
                //noinspection DataFlowIssue
                listeners.forEach(l -> l.singletonUpdateChange(currentValue()));
            } else {
                removeFromDataSource();
            }
        }
        reset();
    }

    @SuppressWarnings("unchecked")
    private T currentValue() {
        return value == NULL_VALUE ? null : (T) value;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Clears the "modified" and cached value, in addition to the super implementation
     */
    @Override
    public void reset() {
        this.value = null;
        super.reset();
    }

    /**
     * Puts the given value into the underlying data source.
     *
     * @param value value to put
     */
    protected abstract void putIntoDataSource(@NonNull T value);

    /**
     * Removes the value related to this singleton from the underlying data source.
     */
    protected abstract void removeFromDataSource();
}
