// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui.hashgraph.internal;

import com.hedera.hapi.platform.event.GossipEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/**
 * Class holding X coordinates for a specific branch for a given node. Note that it's normal the branch to contain
 * events with different generations.
 *
 */
public class BranchCoordinates {

    // the X coordinates of all events in this branch
    private Map<GossipEvent, Integer> xCoordinates;
    // map holding the far most right X coordinate of the branch
    private Map<Long, Integer> generationToMaxX;

    public Map<GossipEvent, Integer> getXCoordinates() {
        return xCoordinates;
    }

    public void setXCoordinates(@NonNull final Map<GossipEvent, Integer> xCoordinates) {
        this.xCoordinates = xCoordinates;
    }

    public Map<Long, Integer> getGenerationToMaxX() {
        return generationToMaxX;
    }

    public void setGenerationToMaxX(@NonNull final Map<Long, Integer> generationToMaxX) {
        this.generationToMaxX = generationToMaxX;
    }
}
