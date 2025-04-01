// SPDX-License-Identifier: Apache-2.0
package com.swirlds.state.merkle.disk;

import static com.swirlds.state.merkle.StateUtils.computeLabel;
import static com.swirlds.state.merkle.StateUtils.getVirtualMapKey;
import static com.swirlds.state.merkle.logging.StateLogger.logQueueAdd;
import static com.swirlds.state.merkle.logging.StateLogger.logQueueRemove;
import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.Codec;
import com.swirlds.state.merkle.queue.QueueNode;
import com.swirlds.state.merkle.queue.QueueState;
import com.swirlds.state.spi.WritableQueueStateBase;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * An implementation of {@link com.swirlds.state.spi.WritableQueueState} based on {@link QueueNode}.
 * @param <E> The type of element in the queue
 */
public class OnDiskWritableQueueState<E> extends WritableQueueStateBase<E> {

    private static final Queue<QueueAction> queueActions = new ConcurrentLinkedQueue<>();


    @NonNull
    private final VirtualMap virtualMap;

    @NonNull
    private final Codec<E> valueCodec;

    @NonNull
    private final OnDiskQueueHelper<E> queueHelper;


    private boolean readyToCommit;


    private enum ActionType{
        ADD, REMOVE
    }

    private record QueueAction<E>(OnDiskWritableQueueState<E> queueState, ActionType actionType, E value){}

    public OnDiskWritableQueueState(
            @NonNull final String serviceName,
            @NonNull final String stateKey,
            @NonNull final Codec<E> valueCodec,
            @NonNull final VirtualMap virtualMap) {
        super(serviceName, stateKey);

        this.virtualMap = Objects.requireNonNull(virtualMap);
        this.valueCodec = requireNonNull(valueCodec);
        this.queueHelper = new OnDiskQueueHelper<>(serviceName, stateKey, virtualMap, valueCodec);
    }

    @Override
    protected void addToDataSource(@NonNull E element) {
        if(readyToCommit) {
            QueueState state = queueHelper.getState();
            if (state == null) {
                // adding 1st time, initialize QueueState
                state = new QueueState();
            }
            try {
                virtualMap.put(getVirtualMapKey(serviceName, stateKey, state.getTailAndIncrement()), element, valueCodec);
                queueHelper.updateState(state);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // Log to transaction state log, what was added
            logQueueAdd(computeLabel(serviceName, stateKey), element);
        } else {
            queueActions.add(new QueueAction<E>(this, ActionType.ADD, element));
        }

    }

    @Override
    protected void removeFromDataSource() {
        if(readyToCommit) {
            final QueueState state = requireNonNull(queueHelper.getState());
            if (!state.isEmpty()) {
                final E removedValue;
                try {
                    removedValue = virtualMap.remove(getVirtualMapKey(serviceName, stateKey, state.getHeadAndIncrement()), valueCodec);
                    queueHelper.updateState(state);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                // Log to transaction state log, what was added
                logQueueRemove(computeLabel(serviceName, stateKey), removedValue);
            } else {
                // TODO: double check, this is according to the logic in `15090-D-fcqueue-to-virtualmap`
                // Log to transaction state log, what was added
                logQueueRemove(computeLabel(serviceName, stateKey), null);
            }
        } else {
            queueActions.add(new QueueAction<E>(this, ActionType.REMOVE, null));
        }

    }

    @NonNull
    @Override
    protected Iterator<E> iterateOnDataSource() {
        return queueHelper.iterateOnDataSource();
    }

    public static void commitAllQueues() {
        while (!queueActions.isEmpty()) {
            QueueAction next = queueActions.remove();
            next.queueState.readyToCommit = true;
            switch (next.actionType) {
                case ADD -> next.queueState.addToDataSource(next.value);
                case REMOVE -> next.queueState.removeFromDataSource();
            }
        }
    }

}
