// SPDX-License-Identifier: Apache-2.0
package com.swirlds.base.telemetry;

/**
 * JFR event for tracing block created event.
 */
@jdk.jfr.Name("consensus.Block")
@jdk.jfr.Label("Consensus Block")
@jdk.jfr.Category({"Hiero"})
public class BlockTrace extends jdk.jfr.Event {
    public enum EventType {
        @jdk.jfr.Label("Created")
        CREATED,
    }
    public long blockNumber;
    public int eventType;
}
