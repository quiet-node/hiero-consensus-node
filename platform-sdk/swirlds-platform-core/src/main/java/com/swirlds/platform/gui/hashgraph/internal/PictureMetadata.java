// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui.hashgraph.internal;

import com.hedera.hapi.platform.event.GossipEvent;
import com.swirlds.platform.gui.hashgraph.HashgraphGuiSource;
import com.swirlds.platform.internal.EventImpl;
import java.awt.Dimension;
import java.awt.FontMetrics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata that is used to aid in drawing a {@code HashgraphPicture}
 */
public class PictureMetadata {
    /**
     * the gap between left side of screen and leftmost column
     * is marginFraction times the gap between columns (and similarly for right side)
     */
    private static final double MARGIN_FRACTION = 0.5;

    private final AddressBookMetadata addressBookMetadata;
    private final int ymax;
    private final int ymin;
    private final int width;
    private final double r;
    private final long minGen;
    private final long maxGen;

    private final HashgraphGuiSource hashgraphSource;
    private Map<Integer, Integer> branchIndexToY;
    private final Map<GossipEvent, Integer> eventToX;
    private final Map<Integer, Map<GossipEvent, Integer>> branchIndexToxCoordinates;

    public PictureMetadata(
            final FontMetrics fm,
            final Dimension pictureDimension,
            final AddressBookMetadata addressBookMetadata,
            final List<EventImpl> events,
            final HashgraphGuiSource hashgraphSource,
            final Map<Integer, Map<GossipEvent, Integer>> branchIndexToxCoordinates,
            final Map<Integer, Integer> branchIndexToY,
            final Map<GossipEvent, Integer> eventToX) {
        this.addressBookMetadata = addressBookMetadata;
        this.hashgraphSource = hashgraphSource;
        this.branchIndexToxCoordinates = branchIndexToxCoordinates;
        this.branchIndexToY = branchIndexToY;
        this.eventToX = eventToX;
        final int fa = fm.getMaxAscent();
        final int fd = fm.getMaxDescent();
        final int textLineHeight = fa + fd;

        width = (int) pictureDimension.getWidth();

        // where to draw next in the window, and the font height
        final int height1 = 0; // text area at the top
        final int height2 = (int) (pictureDimension.getHeight() - height1); // the main display, below the text
        ymin = (int) Math.round(height1 + 0.025 * height2);
        ymax = (int) Math.round(height1 + 0.975 * height2) - textLineHeight;

        long minGenTmp = Long.MAX_VALUE;
        long maxGenTmp = Long.MIN_VALUE;
        for (final EventImpl event : events) {
            minGenTmp = Math.min(minGenTmp, event.getGeneration());
            maxGenTmp = Math.max(maxGenTmp, event.getGeneration());
        }
        maxGenTmp = Math.max(maxGenTmp, minGenTmp + 2);
        minGen = minGenTmp;
        maxGen = maxGenTmp;

        final int n = addressBookMetadata.getNumMembers() + 1;
        final double gens = maxGen - minGen;
        final double dy = (ymax - ymin) * (gens - 1) / gens;
        r = Math.min(width / n / 4, dy / gens / 2);
    }

    /**
     * @return the gap between columns
     */
    public int getGapBetweenColumns() {
        return (int) (width / (addressBookMetadata.getNumColumns() - 1 + 2 * MARGIN_FRACTION));
    }

    /**
     * @return gap between leftmost column and left edge (and similar on right)
     */
    public int getSideGap() {
        return (int) (getGapBetweenColumns() * MARGIN_FRACTION);
    }

    /** find x position on the screen for event e2 which has an other-parent of e1 (or null if none) */
    public int xpos(final EventImpl e1, final EventImpl e2) {
        // the gap between left side of screen and leftmost column
        // is marginFraction times the gap between columns (and similarly for right side)
        final double marginFraction = 0.5;
        // gap between columns
        final int betweenGap = (int) (width / (addressBookMetadata.getNumColumns() - 1 + 2 * marginFraction));
        // gap between leftmost column and left edge (and similar on right)
        final int sideGap = (int) (betweenGap * marginFraction);

        // find the column for e2 next to the column for e1
        int xPos = sideGap + addressBookMetadata.mems2col(e1, e2) * betweenGap;

        final GossipEvent e2GossipEvent = e2.getBaseEvent().getGossipEvent();

        // check if we have a branched event
        if (hashgraphSource.getEventStorage().getBranchIndexMap().containsKey(e2GossipEvent)) {
            final var branchIndex =
                    hashgraphSource.getEventStorage().getBranchIndexMap().get(e2GossipEvent);
            var eventToXCoordinatesForGivenBranch = branchIndexToxCoordinates.get(branchIndex);

            if (eventToXCoordinatesForGivenBranch != null) {
                // event still does not have coordinate
                if (!eventToXCoordinatesForGivenBranch.containsKey(e2GossipEvent)) {
                    // get highest value
                    eventToXCoordinatesForGivenBranch.values().stream()
                            .max(Integer::compareTo)
                            .ifPresent(x -> eventToX.put(e2GossipEvent, x + (int) r));
                    if(eventToX.containsKey(e2GossipEvent)) {
                        xPos = eventToX.get(e2GossipEvent);
                        eventToXCoordinatesForGivenBranch.put(e2GossipEvent, xPos);
                    } else {
                        eventToXCoordinatesForGivenBranch.put(e2GossipEvent, xPos);
                    }
                }
                // event has coordinates
                else {
                    // event goes too much to the right
                    if (eventToXCoordinatesForGivenBranch.size() > 12) {
                        eventToXCoordinatesForGivenBranch.clear();

                        xPos = xPos - sideGap / 4;
                        eventToXCoordinatesForGivenBranch.put(e2GossipEvent, xPos);
                        branchIndexToxCoordinates.put(branchIndex,
                                eventToXCoordinatesForGivenBranch);

                    } else {
                        // event has x coordinate, so just return it
                        xPos = eventToXCoordinatesForGivenBranch.get(e2GossipEvent);
                    }
                }
            } else {
                // that will be the first event of a new branch and coordinates don't exist yet
                eventToXCoordinatesForGivenBranch = new HashMap<>();
                eventToXCoordinatesForGivenBranch.put(e2GossipEvent, xPos);
                branchIndexToxCoordinates.put(branchIndex, eventToXCoordinatesForGivenBranch);
            }
        }

        return xPos;
    }

    /**
     * find y position on the screen for an event
     */
    public int ypos(final EventImpl event) {
        var yPos = (event == null) ? -100 : (int) (ymax - r * (1 + 2 * (event.getGeneration() - minGen)));

        final Map<GossipEvent, Integer> branchIndexMap = hashgraphSource
                .getEventStorage()
                .getBranchIndexMap();
        final GossipEvent gossipEvent = event.getBaseEvent().getGossipEvent();
        if (branchIndexMap.containsKey(gossipEvent)) {
            if (!branchIndexToY.containsKey(branchIndexMap.get(gossipEvent))) {
                branchIndexToY.put(branchIndexMap.get(gossipEvent), yPos);
            } else {
                yPos = branchIndexToY.get(branchIndexMap.get(gossipEvent));
            }
        }

        return yPos;
    }

    /**
     * @return the diameter of a circle representing an event
     */
    public int getD() {
        return (int) (2 * r);
    }

    public int getYmax() {
        return ymax;
    }

    public int getYmin() {
        return ymin;
    }

    /**
     * @return the minimum generation being displayed
     */
    public long getMinGen() {
        return minGen;
    }
}
