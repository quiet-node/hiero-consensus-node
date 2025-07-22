// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.contract.impl.test.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.hedera.node.app.service.contract.impl.exec.utils.OpsDurationThrottleUtils;
import com.hedera.node.app.service.contract.impl.hevm.OpsDurationSchedule;
import org.junit.jupiter.api.Test;

class OpsDurationThrottleUtilsTest {
    @Test
    void testConsumeFromThrottle() {
        final var schedule = mock(OpsDurationSchedule.class);
        OpsDurationThrottleUtils opsDurationThrottleUtils =
                OpsDurationThrottleUtils.withInitiallyAvailableUnits(schedule, 50L);
        assertTrue(opsDurationThrottleUtils.tryConsumeOpsDurationUnits(25L));
        assertEquals(25L, opsDurationThrottleUtils.opsDurationUnitsConsumed());
    }
}
