// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.workflows;

import com.hedera.node.app.workflows.handle.HandleWorkflowModule;
import com.hedera.node.app.workflows.ingest.IngestWorkflowInjectionModule;
import com.hedera.node.app.workflows.prehandle.PreHandleWorkflowInjectionModule;
import com.hedera.node.app.workflows.query.QueryWorkflowInjectionModule;
import com.swirlds.platform.system.InitTrigger;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import javax.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dagger module for all workflows
 */
@Module(
        includes = {
            HandleWorkflowModule.class,
            IngestWorkflowInjectionModule.class,
            PreHandleWorkflowInjectionModule.class,
            QueryWorkflowInjectionModule.class
        })
public interface WorkflowsInjectionModule {
        @Provides
        @Nullable
        @Singleton
        static AtomicBoolean provideMaybeSystemEntitiesCreatedFlag(@NonNull final InitTrigger initTrigger) {
                return initTrigger == InitTrigger.GENESIS ? new AtomicBoolean(false) : null;
        }
}
