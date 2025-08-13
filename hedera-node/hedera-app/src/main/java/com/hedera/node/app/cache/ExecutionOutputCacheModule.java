// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.cache;

import com.hedera.node.app.records.impl.producers.BlockRecordWriterFactory;
import com.hedera.node.config.ConfigProvider;
import com.swirlds.state.lifecycle.info.NetworkInfo;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Singleton;

/**
 * Dagger module for providing the {@link ExecutionOutputCache}.
 */
@Module
public interface ExecutionOutputCacheModule {
    @Provides
    @Singleton
    static ExecutionOutputCache provideRecordBlockCache(
            @NonNull final ConfigProvider configProvider,
            @NonNull NetworkInfo networkInfo,
            @NonNull BlockRecordWriterFactory blockRecordWriterFactory) {
        return new ExecutionOutputCache(configProvider, networkInfo, blockRecordWriterFactory);
    }
}
