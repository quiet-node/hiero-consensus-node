// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.contract.impl.handlers;

import static java.util.Objects.requireNonNull;

import com.hedera.node.app.service.contract.impl.state.WritableEvmHookStore;
import com.hedera.node.app.spi.workflows.HandleContext;
import com.hedera.node.app.spi.workflows.HandleException;
import com.hedera.node.app.spi.workflows.PreCheckException;
import com.hedera.node.app.spi.workflows.PreHandleContext;
import com.hedera.node.app.spi.workflows.PureChecksContext;
import com.hedera.node.app.spi.workflows.TransactionHandler;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class EvmHookDispatchHandler implements TransactionHandler {
    @Inject
    public EvmHookDispatchHandler() {
        // Dagger2
    }

    @Override
    public void pureChecks(@NonNull PureChecksContext context) throws PreCheckException {
        requireNonNull(context);
    }

    @Override
    public void preHandle(@NonNull final PreHandleContext context) throws PreCheckException {
        requireNonNull(context);
    }

    @Override
    public void handle(@NonNull final HandleContext context) throws HandleException {
        requireNonNull(context);
        final var op = context.body().hookDispatchOrThrow();
        switch (op.action().kind()) {
            case HOOK_ID_TO_DELETE -> {
                final var store = context.storeFactory().writableStore(WritableEvmHookStore.class);
                store.markDeleted(op.hookIdToDeleteOrThrow());
            }
            case CREATION -> {
                final var store = context.storeFactory().writableStore(WritableEvmHookStore.class);
                final var creation = op.creationOrThrow();
                store.createEvmHook(creation.entityIdOrThrow(), creation, creation.nextHookId());
            }
            case EXECUTION -> {}
        }
    }
}
