// SPDX-License-Identifier: Apache-2.0
package com.swirlds.base.telemetry;

/**
 * JFR event for tracing consensus transactions.
 */
@jdk.jfr.Name("consensus.Transaction")
@jdk.jfr.Label("Consensus Transaction")
@jdk.jfr.Category({"Hiero"})
public class TransactionTrace extends jdk.jfr.Event {
    public enum EventType {
        @jdk.jfr.Label("Received")
        RECEIVED,
        @jdk.jfr.Label("Executed")
        EXECUTED
    }
    public int txHash; // TransactionID.hashCode() value
    public int eventType; // type of event
}
