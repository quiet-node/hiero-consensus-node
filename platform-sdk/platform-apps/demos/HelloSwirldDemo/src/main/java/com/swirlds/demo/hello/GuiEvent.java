package com.swirlds.demo.hello;

import com.hedera.hapi.platform.event.EventDescriptor;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.platform.internal.EventImpl;
import java.util.Arrays;
import java.util.List;
import org.hiero.base.crypto.Hash;
import org.hiero.consensus.model.event.EventDescriptorWrapper;

public record GuiEvent(
        Bytes id,
        long creator,
        long birthRound,
        long generation,
        List<Bytes> parents,
        long votingRound,
        boolean witness,
        boolean famous,
        boolean judge,
        List<Bytes> stronglySeen
) {

    public static GuiEvent fromEventImpl(final EventImpl eventImpl) {
        return new GuiEvent(
                eventImpl.getBaseHash().getBytes(),
                eventImpl.getCreatorId().id(),
                eventImpl.getBirthRound(),
                eventImpl.getGeneration(),
                eventImpl.getBaseEvent()
                        .getAllParents()
                        .stream()
                        .map(EventDescriptorWrapper::eventDescriptor)
                        .map(EventDescriptor::hash)
                        .toList(),
                eventImpl.getRoundCreated(),
                eventImpl.isWitness(),
                eventImpl.isFamous(),
                eventImpl.isJudge(),
                Arrays.stream(eventImpl.getStronglySeeP())
                        .map(EventImpl::getBaseHash)
                        .map(Hash::getBytes)
                        .toList()
        );
    }
}
