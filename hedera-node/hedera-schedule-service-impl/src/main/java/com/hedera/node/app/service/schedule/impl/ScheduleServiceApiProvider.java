// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.schedule.impl;

import static com.hedera.hapi.node.base.HederaFunctionality.CONTRACT_CALL;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.contract.ContractCallTransactionBody;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.node.app.service.schedule.ScheduleService;
import com.hedera.node.app.service.schedule.ScheduleServiceApi;
import com.hedera.node.app.service.schedule.WritableScheduleStore;
import com.hedera.node.app.service.schedule.impl.handlers.ScheduleCreateHandler;
import com.hedera.node.app.spi.api.ServiceApiProvider;
import com.hedera.node.app.spi.ids.WritableEntityCounters;
import com.hedera.node.config.data.SchedulingConfig;
import com.swirlds.config.api.Configuration;
import com.swirlds.state.spi.WritableStates;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ScheduleServiceApiProvider implements ServiceApiProvider<ScheduleServiceApi> {
    private final ScheduleCreateHandler scheduleCreateHandler;

    @Inject
    public ScheduleServiceApiProvider(@NonNull final ScheduleCreateHandler scheduleCreateHandler) {
        this.scheduleCreateHandler = requireNonNull(scheduleCreateHandler);
    }

    @Override
    public String serviceName() {
        return ScheduleService.NAME;
    }

    @Override
    public ScheduleServiceApi newInstance(
            @NonNull final Configuration configuration,
            @NonNull final WritableStates writableStates,
            @NonNull final WritableEntityCounters entityCounters) {
        requireNonNull(configuration);
        requireNonNull(writableStates);
        requireNonNull(entityCounters);
        return new Impl(
                new WritableScheduleStoreImpl(writableStates, entityCounters),
                configuration.getConfigData(SchedulingConfig.class));
    }

    public class Impl implements ScheduleServiceApi {
        private final WritableScheduleStore scheduleStore;
        private final SchedulingConfig schedulingConfig;

        public Impl(
                @NonNull final WritableScheduleStore scheduleStore, @NonNull final SchedulingConfig schedulingConfig) {
            this.scheduleStore = requireNonNull(scheduleStore);
            this.schedulingConfig = requireNonNull(schedulingConfig);
        }

        @Override
        public boolean hasContractCallCapacity(
                final long consensusSecond, final long gasLimit, @NonNull final AccountID payerId) {
            requireNonNull(payerId);
            final TransactionBody synthBody = TransactionBody.newBuilder()
                    .contractCall(ContractCallTransactionBody.newBuilder().gas(gasLimit))
                    .build();
            final var maybeThrottle =
                    scheduleCreateHandler.loadThrottle(scheduleStore, schedulingConfig, consensusSecond);
            return maybeThrottle
                    .map(throttle ->
                            throttle.allow(payerId, synthBody, CONTRACT_CALL, Instant.ofEpochSecond(consensusSecond)))
                    .orElse(false);
        }
    }
}
