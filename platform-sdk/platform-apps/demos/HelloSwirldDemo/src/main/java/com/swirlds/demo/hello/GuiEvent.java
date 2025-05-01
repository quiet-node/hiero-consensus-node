package com.swirlds.demo.hello;

import com.hedera.hapi.platform.event.EventDescriptor;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.platform.internal.EventImpl;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javafx.scene.paint.Color;
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
        boolean decided,
        boolean consensus,
        List<Bytes> stronglySeen
) {

    public Color color(){
        return EventColor.eventColor(this);
    }

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
                eventImpl.isFameDecided(),
                eventImpl.isConsensus(),
                Optional.ofNullable(eventImpl.getStronglySeeP())
                        .stream()
                        .flatMap(Arrays::stream)
                        .filter(Objects::nonNull)
                        .map(EventImpl::getBaseHash)
                        .map(Hash::getBytes)
                        .toList()
        );
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        addProperty(sb, "hash", id.toHex().substring(0, 6));
        addProperty(sb, "creator", creator);
        addProperty(sb, "birthRound", birthRound);
        addProperty(sb, "generation", generation);
        addProperty(sb, "parents", parents.stream().map(Bytes::toHex).map(s->s.substring(0, 6)).collect(Collectors.joining(",")));
        addProperty(sb, "votingRound", votingRound);
        addProperty(sb, "witness", witness);
        addProperty(sb, "famous", famous);
        addProperty(sb, "judge", judge);
        addProperty(sb, "decided", decided);
        addProperty(sb, "consensus", consensus);
        return sb.toString();
    }

    private void addProperty(final StringBuilder sb, final String name, final Object value) {
        sb.append("%-15s".formatted(name))
                .append(" ")
                .append(value)
                .append("\n");
    }
}
