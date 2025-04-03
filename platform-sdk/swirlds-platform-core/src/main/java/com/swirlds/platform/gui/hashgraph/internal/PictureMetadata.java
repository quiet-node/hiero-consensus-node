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
    private final Map<Integer, Integer> branchIndexToY;
    private final Map<Integer, Integer> branchIndexToMinimumXCoordinate;
    private final Map<Integer, Map<GossipEvent, Integer>> branchIndexToAllXCoordinates;
    private final Map<Integer, Map<GossipEvent, Integer>> branchIndexToCurrentXCoordinates;

    /**
     *
     * @param fm font metrics to use for visualisation
     * @param pictureDimension the dimension of the UI component that will be used
     * @param addressBookMetadata metadata for the address book
     * @param events the events to be displayed
     * @param hashgraphSource the needed information for visualisation from the hashgraph to use as a source
     * @param branchIndexToY map collecting Y coordinates for each branch
     * @param branchIndexToMinimumXCoordinate map collecting minimum X coordinate (far most left) for each branch
     * @param allBranchedXCoordinates map collecting X coordinates for all branched events
     * @param displayedBranchedXCoordinates map collecting X coordinates for displayed branched events
     *
     */
    public PictureMetadata(
            final FontMetrics fm,
            final Dimension pictureDimension,
            final AddressBookMetadata addressBookMetadata,
            final List<EventImpl> events,
            final HashgraphGuiSource hashgraphSource,
            final Map<Integer, Integer> branchIndexToY,
            final Map<Integer, Integer> branchIndexToMinimumXCoordinate,
            final Map<Integer, Map<GossipEvent, Integer>> allBranchedXCoordinates,
            final Map<Integer, Map<GossipEvent, Integer>> displayedBranchedXCoordinates) {
        this.addressBookMetadata = addressBookMetadata;
        this.hashgraphSource = hashgraphSource;
        this.branchIndexToAllXCoordinates = allBranchedXCoordinates;
        this.branchIndexToCurrentXCoordinates = displayedBranchedXCoordinates;
        this.branchIndexToY = branchIndexToY;
        this.branchIndexToMinimumXCoordinate = branchIndexToMinimumXCoordinate;
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
        if (hashgraphSource.getEventStorage().getBranchIndexes().containsKey(e2GossipEvent)) {
            final var branchIndex =
                    hashgraphSource.getEventStorage().getBranchIndexes().get(e2GossipEvent);

            var eventToXCoordinatesForDisplayedBranchEvents = branchIndexToCurrentXCoordinates.get(branchIndex);

            if (eventToXCoordinatesForDisplayedBranchEvents != null) {
                // event still does not have X coordinate
                if (!eventToXCoordinatesForDisplayedBranchEvents.containsKey(e2GossipEvent)) {
                    // get highest X coordinate from existing branch events and add an offset
                    final int maxXCoordinateForBranch = eventToXCoordinatesForDisplayedBranchEvents.values().stream()
                            .max(Integer::compareTo)
                            .orElse(-1);
                    if (maxXCoordinateForBranch > 0) {
                        xPos = maxXCoordinateForBranch + (int) r;
                    }

                    final var eventToXCoordinatesForAllBranchedEvents = branchIndexToAllXCoordinates.get(branchIndex);
                    eventToXCoordinatesForAllBranchedEvents.put(e2GossipEvent, xPos);
                    branchIndexToAllXCoordinates.put(branchIndex, eventToXCoordinatesForAllBranchedEvents);
                }
                // event has X coordinate
                else {
                    // events have gone too much to the right, so shift them all to the left
                    if (eventToXCoordinatesForDisplayedBranchEvents.size() > 12) {
                        eventToXCoordinatesForDisplayedBranchEvents.clear();

                        xPos = branchIndexToMinimumXCoordinate.get(branchIndex);

//                        final var allBranchXCoordinates = branchIndexToAllXCoordinates.get(branchIndex);
//                        allBranchXCoordinates.put(e2GossipEvent, xPos);
//                        branchIndexToAllXCoordinates.put(branchIndex, allBranchXCoordinates);

                        eventToXCoordinatesForDisplayedBranchEvents.put(e2GossipEvent, xPos);
                        branchIndexToAllXCoordinates.put(branchIndex, eventToXCoordinatesForDisplayedBranchEvents);
                        branchIndexToCurrentXCoordinates.put(branchIndex, eventToXCoordinatesForDisplayedBranchEvents);

                    } else {
                        // event has X coordinate and it is in proper position, so just assign it
                        xPos = eventToXCoordinatesForDisplayedBranchEvents.get(e2GossipEvent);
                    }
                }
            } else {
                // the event will be the first one of a new branch and X coordinate for it doesn't exist yet
                final var allBranchXCoordinates = new HashMap<GossipEvent, Integer>();
                allBranchXCoordinates.put(e2GossipEvent, xPos);
                branchIndexToAllXCoordinates.put(branchIndex, allBranchXCoordinates);

                // mark the first X position for the branch, so that it's used when branched events are shifted /
                // reloaded
                branchIndexToMinimumXCoordinate.put(branchIndex, xPos);
            }
        }

        return xPos;
    }

    /**
     * find y position on the screen for an event
     */
    public int ypos(final EventImpl event) {
        var yPos = (event == null) ? -100 : (int) (ymax - r * (1 + 2 * (event.getGeneration() - minGen)));

        final Map<GossipEvent, Integer> allBranchedEvents =
                hashgraphSource.getEventStorage().getBranchIndexes();
        final GossipEvent gossipEvent = event.getBaseEvent().getGossipEvent();
        if (allBranchedEvents.containsKey(gossipEvent)) {
            if (!branchIndexToY.containsKey(allBranchedEvents.get(gossipEvent))) {
                branchIndexToY.put(allBranchedEvents.get(gossipEvent), yPos);
            } else {
                yPos = branchIndexToY.get(allBranchedEvents.get(gossipEvent));
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
