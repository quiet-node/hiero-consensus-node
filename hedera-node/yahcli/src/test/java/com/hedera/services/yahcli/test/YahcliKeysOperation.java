// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.yahcli.test;

import static java.util.Objects.requireNonNull;

import com.hedera.node.app.hapi.utils.EntityType;
import com.hedera.services.bdd.spec.HapiSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public class YahcliKeysOperation extends AbstractYahcliOperation<YahcliKeysOperation> {
    @Nullable
    private Consumer<Map<EntityType, Set<Long>>> keyFilesCb;

    @Override
    protected YahcliKeysOperation self() {
        return this;
    }

    public YahcliKeysOperation withKeyFilesCb(@NonNull final Consumer<Map<EntityType, Set<Long>>> keyFilesCb) {
        this.keyFilesCb = requireNonNull(keyFilesCb);
        return this;
    }

    @Override
    public Optional<Throwable> execFor(@NonNull HapiSpec spec) {
        try {

            return Optional.empty();
        } catch (Throwable t) {
            return Optional.of(t);
        }
    }

    private void readKeyFiles() {
        final var w = workingDirOrThrow();
    }
}
