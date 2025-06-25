// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.network.protocol.rpc;

import com.hedera.hapi.platform.event.GossipEvent;
import com.hedera.hapi.platform.message.GossipPing;
import com.swirlds.platform.gossip.rpc.GossipRpcSender;
import com.swirlds.platform.gossip.rpc.SyncData;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MagicQueueSystem implements GossipRpcSender {

    public final BlockingQueue<SenderCall> outputQueue = new LinkedBlockingQueue<>();
    public final BlockingQueue<Runnable> inputQueue = new LinkedBlockingQueue<>();

    @Override
    public void sendSyncData(@NonNull final SyncData syncMessage) {
        outputQueue.add(new SyncDataCall(syncMessage));
    }

    @Override
    public void sendTips(@NonNull final List<Boolean> tips) {
        outputQueue.add(new TipsCall(tips));
    }

    @Override
    public void sendEvents(@NonNull final List<GossipEvent> gossipEvents) {
        outputQueue.add(new EventsCall(gossipEvents));
    }

    @Override
    public void sendEndOfEvents() {
        outputQueue.add(new EndOfEventsCall());
    }

    @Override
    public void breakConversation() {
        outputQueue.add(new BreakConversationCall());
    }

    public void sendPingReply(final GossipPing reply) {
        outputQueue.add(new PingReplyCall(reply));
    }
}
