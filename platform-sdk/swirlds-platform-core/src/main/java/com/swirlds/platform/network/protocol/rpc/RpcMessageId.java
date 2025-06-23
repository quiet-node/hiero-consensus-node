// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.network.protocol.rpc;

/**
 * Internal {@link RpcPeerProtocol} byte indicators for various message types
 */
class RpcMessageId {

    /**
     * Indicates GossipSyncData from send/receiveSyncData
     */
    static final int SYNC_DATA = 1;

    /**
     * Indicates GossipKnowTips from send/receiveTips
     */
    static final int KNOWN_TIPS = 2;

    /**
     * Indicates single GossipEvent from send/receiveEvents
     */
    static final int EVENT = 3;

    /**
     * Indicates that stream of events is finished, from send/receivedEventsFinished
     */
    static final int EVENTS_FINISHED = 4;

    /**
     * Indicates GossipPing ping request
     */
    static final int PING = 5;

    /**
     * Indicates GossipPing ping reply
     */
    static final int PING_REPLY = 6;
}
