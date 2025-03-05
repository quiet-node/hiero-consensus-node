/*
 * Copyright (C) 2023-2025 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.swirlds.state.merkle.queue;

import static com.swirlds.state.merkle.StateUtils.computeLabel;
import static com.swirlds.state.merkle.logging.StateLogger.logQueueAdd;
import static com.swirlds.state.merkle.logging.StateLogger.logQueueRemove;
import static java.util.Objects.requireNonNull;

import com.swirlds.state.spi.WritableQueueStateBase;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;

/**
 * An implementation of {@link com.swirlds.state.spi.WritableQueueState} based on {@link QueueNode}.
 * @param <E> The type of element in the queue
 */
public class BackedWritableQueueState<E> extends WritableQueueStateBase<E> {

    private final QueueNode<E> dataSource;

    public BackedWritableQueueState(
            @NonNull final String serviceName, @NonNull final String stateKey, @NonNull final QueueNode<E> node) {
        super(serviceName, stateKey);
        this.dataSource = requireNonNull(node);
    }

    @NonNull
    @Override
    protected Iterator<E> iterateOnDataSource() {
        return dataSource.iterator();
    }

    @Override
    protected void addToDataSource(@NonNull E element) {
        dataSource.add(element);
        // Log to transaction state log, what was added
        logQueueAdd(computeLabel(serviceName, stateKey), element);
    }

    @Override
    protected void removeFromDataSource() {
        final var removedValue = dataSource.remove();
        // Log to transaction state log, what was added
        logQueueRemove(computeLabel(serviceName, stateKey), removedValue);
    }
}
