// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.hapi.utils.throttles;

import static com.hedera.node.app.hapi.utils.throttles.BucketThrottle.CAPACITY_UNITS_PER_TXN;
import static com.hedera.node.app.hapi.utils.throttles.DeterministicThrottle.nanosBetween;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.throttles.ThrottleUsageSnapshot;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;

/**
 * Main class responsible for throttling transactions by gasLimit. Keeps track of the instance the
 * last decision was made and calculates the time elapsed since then. Uses a {@link
 * GasLimitBucketThrottle} under the hood.
 */
public class GasLimitDeterministicThrottle implements CongestibleThrottle {
    private static final String THROTTLE_NAME = "Gas";
    private final DeterministicThrottle delegate;
    private Timestamp lastDecisionTime;

    /**
     * Creates a new instance of the throttle with capacity - the total amount of gas allowed per
     * sec.
     *
     * @param capacity - the total amount of gas allowed per sec.
     * @param burstPeriodMs - the burst period as milliseconds
     */
    public GasLimitDeterministicThrottle(final long capacity, final long burstPeriodMs) {
        this.delegate = DeterministicThrottle.withTpsAndBurstPeriodMsNamed(
                Math.toIntExact(capacity), burstPeriodMs, THROTTLE_NAME);
    }

    /**
     * Calculates the amount of nanoseconds that elapsed since the last time the method was called.
     * Verifies whether there is enough capacity to handle a transaction with some gasLimit.
     *
     * @param now        - the instant against which the {@link GasLimitBucketThrottle} is tested.
     * @param txGasLimit - the gasLimit extracted from the transaction payload.
     * @return true if there is enough capacity to handle this transaction; false if it should be
     * throttled.
     */
    public boolean allow(@NonNull final Instant now, final long txGasLimit) {
        final var elapsedNanos = nanosBetween(lastDecisionTime, now);
        if (elapsedNanos < 0L) {
            throw new IllegalArgumentException("Throttle timeline must advance, but " + now + " is not after "
                    + Instant.ofEpochSecond(lastDecisionTime.seconds(), lastDecisionTime.nanos()));
        }
        if (txGasLimit < 0) {
            throw new IllegalArgumentException("Gas limit must be non-negative, but was " + txGasLimit);
        }
        lastDecisionTime = new Timestamp(now.getEpochSecond(), now.getNano());
        return delegate.allow(Math.toIntExact(txGasLimit), now);
    }

    /**
     * Returns the free-to-used ratio in the bucket at its last decision time.
     *
     * @return the free-to-used ratio at that time
     */
    public long instantaneousFreeToUsedRatio() {
        final var used = delegate.used() / CAPACITY_UNITS_PER_TXN;
        return (used == 0) ? Long.MAX_VALUE : capacityFree() / used;
    }

    /**
     * Returns the percent usage of this throttle, at a time which may be later than the last
     * throttling decision (which would imply some capacity has been freed).
     *
     * @param now a time which will be ignored if before the last throttling decision
     * @return the capacity available at this time
     */
    public double percentUsed(final Instant now) {
        if (lastDecisionTime == null) {
            return 0.0;
        }
        return delegate.percentUsed(now);
    }

    /**
     * Returns the percent usage of this throttle, at a time which may be later than the last
     * throttling decision (which would imply some capacity has been freed).
     *
     * @return the capacity available at this time
     */
    @Override
    public double instantaneousPercentUsed() {
        if (lastDecisionTime == null) {
            return 0.0;
        }
        return delegate.instantaneousPercentUsed();
    }

    /**
     * Returns the capacity of the throttle.
     *
     * @return the capacity of the throttle
     */
    @Override
    public long capacity() {
        return delegate.capacity() / CAPACITY_UNITS_PER_TXN;
    }

    @Override
    @SuppressWarnings("java:S125")
    public long mtps() {
        // We treat the "milli-TPS" of the throttle bucket as 1000x its gas/sec;
        return capacity() * 1_000;
    }

    @Override
    public String name() {
        return THROTTLE_NAME;
    }

    /**
     * Returns the used capacity of the throttle.
     *
     * @return the used capacity of the throttle
     */
    @Override
    public long used() {
        return delegate.used() / CAPACITY_UNITS_PER_TXN;
    }

    /**
     * Used to release some capacity previously reserved by calling {@link
     * GasLimitDeterministicThrottle#allow(Instant, long)} without having to wait for the natural
     * leakage.
     *
     * @param value - the amount to release
     */
    public void leakUnusedGasPreviouslyReserved(long value) {
        delegate.leakCapacity(value * CAPACITY_UNITS_PER_TXN);
    }

    public ThrottleUsageSnapshot usageSnapshot() {
        return new ThrottleUsageSnapshot(delegate.used() / CAPACITY_UNITS_PER_TXN, lastDecisionTime);
    }

    public void resetUsageTo(@NonNull final ThrottleUsageSnapshot usageSnapshot) {
        requireNonNull(usageSnapshot);
        lastDecisionTime = usageSnapshot.lastDecisionTime();
        final ThrottleUsageSnapshot adjustedSnapshot =
                new ThrottleUsageSnapshot(usageSnapshot.used() * CAPACITY_UNITS_PER_TXN, lastDecisionTime);
        delegate.resetUsageTo(adjustedSnapshot);
    }

    public void resetUsage() {
        delegate.resetUsage();
    }

    public void reclaimLastAllowedUse() {
        delegate.reclaimLastAllowedUse();
    }

    public void resetLastAllowedUse() {
        delegate.resetLastAllowedUse();
    }

    public long capacityFree() {
        return delegate.capacityFree() / CAPACITY_UNITS_PER_TXN;
    }
}
