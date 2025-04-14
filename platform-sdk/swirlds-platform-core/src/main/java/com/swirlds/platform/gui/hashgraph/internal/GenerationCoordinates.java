// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui.hashgraph.internal;

import com.hedera.hapi.platform.event.GossipEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/**
 * Class holding X coordinates for branched events with a specific generation for a given node. Note that it's normal the branch to contain
 * events with different generations.
 *
 */
public class GenerationCoordinates {

    // the X coordinates of all events in this branch
    private Map<GossipEvent, Integer> xCoordinates;
    // the most right X coordinate of the branch
    private Integer maxX = 0;

    public Map<GossipEvent, Integer> getXCoordinates() {
        return xCoordinates;
    }

    public void setXCoordinates(@NonNull final Map<GossipEvent, Integer> xCoordinates) {
        this.xCoordinates = xCoordinates;
    }

    public Integer getMaxX() {
        return maxX;
    }

    public void setMaxX(@NonNull final Integer maxX) {
        this.maxX = maxX;
    }
}
