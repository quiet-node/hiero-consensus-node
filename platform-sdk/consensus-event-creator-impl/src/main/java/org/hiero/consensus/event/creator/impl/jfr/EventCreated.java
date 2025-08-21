package org.hiero.consensus.event.creator.impl.jfr;

import com.hedera.hapi.platform.event.EventCore;
import com.hedera.hapi.platform.event.GossipEvent;
import java.time.Instant;
import jdk.jfr.Timestamp;
import org.hiero.consensus.model.event.PlatformEvent;

@jdk.jfr.Name("consensus.EventCreated")
@jdk.jfr.Label("Consensus Event Created")
@jdk.jfr.Category({"Consensus"})
public class EventCreated extends jdk.jfr.Event {
    int creatorNodeId;
    long birthRound;
    int  txCount; // number of transactions in the event
    long longHash; // long hash of the event, first 8 bytes of the SHA384 hash
    @Timestamp long localCreatedTimeStamp;

    public void setAll(PlatformEvent platformEvent) {
        final GossipEvent gossipEvent = platformEvent.getGossipEvent();
        final EventCore eventCore = gossipEvent.eventCore();
        final Instant timeReceived = platformEvent.getTimeReceived();
        birthRound = eventCore.birthRound();
        creatorNodeId = (int)eventCore.creatorNodeId();
        txCount = platformEvent.getTransactionCount();
        localCreatedTimeStamp =  timeReceived.getEpochSecond() * 1_000_000_000L + timeReceived.getNano();
        longHash = platformEvent.getHash().getBytes().getLong(0);
    }
}
