// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tracks information for a block node across multiple connection instances.
 * This data persists beyond individual BlockNodeConnection lifecycles to properly
 * implement rate limiting and health monitoring.
 */
public class BlockNodeStats {
    /**
     * Queue for tracking EndOfStream response timestamps for rate limiting.
     */
    private final Queue<Instant> endOfStreamTimestamps = new ConcurrentLinkedQueue<>();

    /**
     * Records a new EndOfStream response timestamp.
     * @param timestamp when the EndOfStream was received
     */
    void recordEndOfStream(Instant timestamp) {
        endOfStreamTimestamps.add(timestamp);
    }

    /**
     * Checks if the EndOfStream rate limit has been exceeded within the given timeframe.
     * @param maxAllowed maximum number of EndOfStream responses allowed
     * @param timeFrame time window to check
     * @return true if rate limit exceeded
     */
    boolean hasExceededEndOfStreamLimit(int maxAllowed, Duration timeFrame) {
        final Instant now = Instant.now();
        final Instant cutoff = now.minus(timeFrame);

        // Remove expired timestamps
        final Iterator<Instant> it = endOfStreamTimestamps.iterator();
        while (it.hasNext()) {
            final Instant timestamp = it.next();
            if (timestamp.isBefore(cutoff)) {
                it.remove();
            } else {
                break;
            }
        }
        return endOfStreamTimestamps.size() > maxAllowed;
    }
}
