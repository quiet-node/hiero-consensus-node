// SPDX-License-Identifier: Apache-2.0
package com.swirlds.base.telemetry;

/**
 * JFR event for tracing consensus rounds.
 */
@jdk.jfr.Name("consensus.Round")
@jdk.jfr.Label("Consensus Round")
@jdk.jfr.Category({"Hiero"})
public class RoundTrace extends jdk.jfr.Event {
    public enum EventType {
        @jdk.jfr.Label("Created")
        CREATED,
        @jdk.jfr.Label("Executed")
        EXECUTED
    }
    public long roundNum; // round number
    public int eventType; // type of event
}
