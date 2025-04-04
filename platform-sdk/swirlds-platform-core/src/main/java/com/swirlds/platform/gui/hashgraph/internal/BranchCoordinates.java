// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui.hashgraph.internal;

import com.hedera.hapi.platform.event.GossipEvent;
import java.util.Map;

public class BranchCoordinates {

    private int y;
    private int farMostLeftX;
    private Map<GossipEvent, Integer> allXCoordinates;
    private Map<GossipEvent, Integer> insideGenerationRangeXCoordinates;

    public int getY() {
        return y;
    }

    public int getFarMostLeftX() {
        return farMostLeftX;
    }

    public Map<GossipEvent, Integer> getAllXCoordinates() {
        return allXCoordinates;
    }

    public Map<GossipEvent, Integer> getInsideGenerationRangeXCoordinates() {
        return insideGenerationRangeXCoordinates;
    }

    public void setY(int y) {
        this.y = y;
    }

    public void setFarMostLeftX(int farMostLeftX) {
        this.farMostLeftX = farMostLeftX;
    }

    public void setEventToXCoordinatesForAllBranchedEvents(Map<GossipEvent, Integer> allXCoordinates) {
        this.allXCoordinates = allXCoordinates;
    }

    public void setInsideGenerationRangeXCoordinates(Map<GossipEvent, Integer> insideGenerationRangeXCoordinates) {
        this.insideGenerationRangeXCoordinates = insideGenerationRangeXCoordinates;
    }
}
