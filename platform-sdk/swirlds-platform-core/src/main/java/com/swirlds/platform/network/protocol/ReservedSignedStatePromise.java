// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.network.protocol;

import com.swirlds.platform.state.signed.ReservedSignedState;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.base.concurrent.BlockingResourceProvider;

public class ReservedSignedStatePromise {
    private final BlockingResourceProvider<ReservedSignedState> provider = new BlockingResourceProvider<>();

    public void provide(@NonNull final ReservedSignedState currentReservedSignedState) throws InterruptedException {
        this.provider.provide(currentReservedSignedState);
    }

    public boolean acquire() {
        return provider.acquireProvidePermit();
    }

    public boolean tryBlock() {
        return provider.tryBlockProvidePermit();
    }

    public void release() {
        provider.releaseProvidePermit();
    }

    public ReservedSignedState get() {
        try (var lock = provider.waitForResource()) {
            return lock.getResource();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
