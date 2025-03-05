// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.workflows.standalone.impl;

import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.platform.state.PlatformState;
import com.hedera.node.app.annotations.NodeSelfId;
import com.hedera.node.app.metrics.StoreMetricsServiceImpl;
import com.hedera.node.app.spi.metrics.StoreMetricsService;
import com.hedera.node.app.version.ServicesSoftwareVersion;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.state.PlatformStateAccessor;
import com.swirlds.platform.state.service.SnapshotPlatformStateAccessor;
import com.swirlds.state.lifecycle.EntityIdFactory;
import com.swirlds.state.lifecycle.info.NetworkInfo;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.InstantSource;
import java.util.function.IntSupplier;
import javax.inject.Singleton;

@Module
public interface StandaloneModule {
    @Binds
    @Singleton
    NetworkInfo bindNetworkInfo(@NonNull StandaloneNetworkInfo simulatedNetworkInfo);

    @Provides
    @Singleton
    static IntSupplier provideFrontendThrottleSplit() {
        return () -> 1;
    }

    @Provides
    @Singleton
    static PlatformStateAccessor providePlatformState() {
        return new SnapshotPlatformStateAccessor(PlatformState.DEFAULT, ServicesSoftwareVersion::new);
    }

    @Provides
    @Singleton
    static InstantSource provideInstantSource() {
        return InstantSource.system();
    }

    @Provides
    @Singleton
    @NodeSelfId
    static AccountID provideNodeSelfId(EntityIdFactory entityIdFactory) {
        // This is only used to check the shard and realm of account ids
        return entityIdFactory.newDefaultAccountId();
    }

    @Provides
    @Singleton
    static StoreMetricsService provideStoreMetricsService(Metrics metrics) {
        return new StoreMetricsServiceImpl(metrics);
    }
}
