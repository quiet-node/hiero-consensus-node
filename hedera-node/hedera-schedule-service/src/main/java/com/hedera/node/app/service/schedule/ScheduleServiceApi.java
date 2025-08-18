package com.hedera.node.app.service.schedule;

import com.hedera.hapi.node.base.AccountID;

/**
 * Additional API for the Schedule Service beyond its dispatchable handlers.
 */
public interface ScheduleServiceApi {
    /**
     * Checks if the schedule service has enough capacity to process a contract call
     * at the given consensus second with the specified gas limit.
     *
     * @param consensusSecond the consensus second at which the call is to be processed
     * @param gasLimit the gas limit for the contract call
     * @param payerId the ID of the account that will pay for the contract call
     * @return true if the schedule service has enough capacity to process the contract call,
     */
    boolean hasContractCallCapacity(long consensusSecond, long gasLimit, AccountID payerId);
}
