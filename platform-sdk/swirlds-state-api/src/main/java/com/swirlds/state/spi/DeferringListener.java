// SPDX-License-Identifier: Apache-2.0
package com.swirlds.state.spi;

import static com.swirlds.logging.legacy.LogMarker.RECONNECT;
import static java.util.Objects.requireNonNull;

import com.swirlds.state.StateChangeListener.StateType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A listener that can defer committing changes to the state with which they are associated.
 */
public interface DeferringListener {
    Logger logger = LogManager.getLogger(DeferringListener.class);
    /**
     * Whether the state with which this listener is associated should defer committing changes.
     */
    default boolean deferCommits() {
        return false;
    }

    /**
     * The target state types that the listener is interested in.
     * @return the target state types
     */
    Set<StateType> stateTypes();

    /**
     * Called when a commit is deferred.
     */
    default void commitDeferred() {}

    /**
     * Returns the agreed defer commit setting for the given listeners. If the listeners have
     * inconsistent defer commit settings, an exception is thrown.
     *
     * @param listeners the list of listeners to check
     * @param stateType expected state type
     * @return true if all listeners agree to defer commits, false otherwise
     * @throws IllegalArgumentException if the listeners have inconsistent defer commit settings
     */
    static <T extends DeferringListener> boolean agreedDeferCommitOrThrow(
            @NonNull final List<T> listeners, StateType stateType) {
        requireNonNull(listeners);
        final List<T> filteredListeners = listeners.stream()
                .filter(l -> l.stateTypes().contains(stateType))
                .toList();
        boolean deferCommits = false;

        logger.info(RECONNECT.getMarker(), "State change listener info for state type {}: ", stateType);
        for (T filteredListener : filteredListeners) {
            logger.info(
                    RECONNECT.getMarker(),
                    "Listener type: {}. State types: {} Defer commits: {}",
                    filteredListener.getClass().getName(),
                    filteredListener.stateTypes(),
                    filteredListener.deferCommits());
        }

        if (!filteredListeners.isEmpty()) {
            deferCommits = filteredListeners.getFirst().deferCommits();
            if (filteredListeners.size() > 1) {
                final var restAgree = filteredListeners.subList(1, filteredListeners.size()).stream()
                        .allMatch(l ->
                                l.deferCommits() == filteredListeners.getFirst().deferCommits());
                if (!restAgree) {
                    throw new IllegalArgumentException("Listeners have inconsistent defer commit settings");
                }
            }
        }
        return deferCommits;
    }
}
