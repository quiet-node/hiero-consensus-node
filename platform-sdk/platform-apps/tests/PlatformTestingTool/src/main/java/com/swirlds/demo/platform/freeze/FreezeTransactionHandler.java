// SPDX-License-Identifier: Apache-2.0
package com.swirlds.demo.platform.freeze;

import com.hedera.hapi.platform.state.PlatformState;
import com.swirlds.demo.platform.fs.stresstest.proto.FreezeTransaction;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.state.State;
import java.time.Instant;

import com.swirlds.state.spi.WritableSingletonState;
import com.swirlds.state.spi.WritableSingletonStateBase;
import com.swirlds.state.spi.WritableStates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import static com.swirlds.platform.state.service.PlatformStateService.NAME;
import static com.swirlds.platform.state.service.schemas.V0540PlatformStateSchema.PLATFORM_STATE_KEY;
import static org.hiero.base.utility.CommonUtils.toPbjTimestamp;

public class FreezeTransactionHandler {
    private static final Logger logger = LogManager.getLogger(FreezeTransactionHandler.class);
    private static final Marker LOGM_FREEZE = MarkerManager.getMarker("FREEZE");

    public static boolean freeze(
            final FreezeTransaction transaction, final PlatformStateFacade platformStateFacade, final State state) {
        logger.debug(LOGM_FREEZE, "Handling FreezeTransaction: " + transaction);
        try {

            final Instant freezeTime = Instant.ofEpochSecond(transaction.getStartTimeEpochSecond());
            final WritableStates platformStates = state.getWritableStates(NAME);
            final WritableSingletonState<PlatformState> writablePlatformState = platformStates.getSingleton(PLATFORM_STATE_KEY);
            writablePlatformState.put(writablePlatformState.get().copyBuilder().freezeTime(toPbjTimestamp(freezeTime)).build());
            ((WritableSingletonStateBase<?>)writablePlatformState).commit();
            return true;
        } catch (IllegalArgumentException ex) {
            logger.warn(LOGM_FREEZE, "FreezeTransactionHandler::freeze fails. {}", ex.getMessage());
            return false;
        }
    }
}
