package com.swirlds.platform.test.fixtures.fxui;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;

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
}
