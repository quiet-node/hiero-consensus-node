// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.spec.transactions.lambda;

import static com.hedera.hapi.node.hooks.HookExtensionPoint.ACCOUNT_ALLOWANCE_HOOK;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.noOp;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.node.hooks.EvmHookSpec;
import com.hedera.hapi.node.hooks.HookCreationDetails;
import com.hedera.hapi.node.hooks.LambdaEvmHook;
import com.hedera.hapi.node.hooks.PureEvmHook;
import com.hedera.hapi.node.state.hooks.EvmHookType;
import com.hedera.services.bdd.spec.SpecOperation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory class for creating {@link com.hedera.hapi.node.hooks.HookCreationDetails}s.
 */
public class HookCreator {
    private final long hookId;
    private final EvmHookType type;

    /**
     * Returns a {@link HookCreator} for a lambda hook using the given index.
     * @param hookId the index of the hook
     * @return a {@link HookCreator} for a lambda hook
     */
    public static HookCreator lambdaAt(final long hookId) {
        return new HookCreator(EvmHookType.LAMBDA, hookId);
    }

    private HookCreator(@NonNull final EvmHookType type, final long hookId) {
        this.type = requireNonNull(type);
        this.hookId = hookId;
    }

    public SpecOperation specSetupOp() {
        return noOp();
    }

    /**
     * Returns the {@link com.hedera.hapi.node.hooks.HookCreation}.
     */
    public HookCreationDetails getCreationDetails() {
        final var builder = HookCreationDetails.newBuilder().hookId(hookId).extensionPoint(ACCOUNT_ALLOWANCE_HOOK);
        final var specBuilder = EvmHookSpec.newBuilder();
        switch (type) {
            case PURE -> builder.pureEvmHook(new PureEvmHook(specBuilder.build()));
            case LAMBDA ->
                builder.lambdaEvmHook(
                        LambdaEvmHook.newBuilder().spec(specBuilder).build());
        }
        return builder.build();
    }
}
