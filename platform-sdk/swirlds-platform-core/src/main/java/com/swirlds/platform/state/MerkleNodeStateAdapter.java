/*
 * Copyright (C) 2025 Hedera Hashgraph, LLC
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

package com.swirlds.platform.state;

import com.swirlds.base.time.Time;
import com.swirlds.common.constructable.ConstructableIgnored;
import com.swirlds.common.crypto.Hash;
import com.swirlds.common.merkle.MerkleNode;
import com.swirlds.common.merkle.crypto.MerkleCryptography;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.lifecycle.StateMetadata;
import com.swirlds.state.spi.ReadableStates;
import com.swirlds.state.spi.WritableStates;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

@ConstructableIgnored
public class MerkleNodeStateAdapter implements MerkleNodeState {

    private final MerkleNodeState delegate;

    public MerkleNodeStateAdapter(MerkleNodeState delegate) {
        this.delegate = delegate;
    }

    @NonNull
    @Override
    public MerkleNodeStateAdapter copy() {
        return new MerkleNodeStateAdapter(delegate.copy());
    }

    @Override
    public <T extends MerkleNode> void putServiceStateIfAbsent(
            @NonNull StateMetadata<?, ?> md, @NonNull Supplier<T> nodeSupplier, @NonNull Consumer<T> nodeInitializer) {
        delegate.putServiceStateIfAbsent(md, nodeSupplier, nodeInitializer);
    }

    @Override
    public void unregisterService(@NonNull String serviceName) {
        delegate.unregisterService(serviceName);
    }

    @Override
    public void removeServiceState(@NonNull String serviceName, @NonNull String stateKey) {
        delegate.removeServiceState(serviceName, stateKey);
    }

    @Override
    public boolean release() {
        return delegate.release();
    }

    @Override
    public boolean isDestroyed() {
        return delegate.isDestroyed();
    }

    @Override
    public void init(
            @NonNull final Configuration configuration,
            Time time,
            Metrics metrics,
            MerkleCryptography merkleCryptography,
            LongSupplier roundSupplier) {
        delegate.init(configuration, time, metrics, merkleCryptography, roundSupplier);
    }

    @NonNull
    @Override
    public ReadableStates getReadableStates(@NonNull String serviceName) {
        return delegate.getReadableStates(serviceName);
    }

    @NonNull
    @Override
    public WritableStates getWritableStates(@NonNull String serviceName) {
        return delegate.getWritableStates(serviceName);
    }

    @Override
    public void setHash(Hash hash) {
        delegate.setHash(hash);
    }
}
