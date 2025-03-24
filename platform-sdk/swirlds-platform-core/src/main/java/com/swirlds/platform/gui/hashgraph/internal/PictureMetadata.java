// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui.hashgraph.internal;

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
    private final  Map<Long, Integer> branchIndexToX;
    private final  Map<Long, Integer> branchIndexToY;
    private final Map<EventImpl, Integer> eventToX;
    private int lastX;

    public PictureMetadata(
            final FontMetrics fm,
            final Dimension pictureDimension,
            final AddressBookMetadata addressBookMetadata,
            final List<EventImpl> events,
            final Map<Long, Integer> branchIndexToX,
            final Map<Long, Integer> branchIndexToY,
            final Map<EventImpl, Integer> eventToX) {
        this.branchIndexToX = branchIndexToX;
        this.branchIndexToY = branchIndexToY;
        this.eventToX = eventToX;
        this.addressBookMetadata = addressBookMetadata;
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
    public int xpos(final EventImpl e1, final EventImpl e2, final boolean isText) {
                // the gap between left side of screen and leftmost column
                // is marginFraction times the gap between columns (and similarly for right side)
                final double marginFraction = 0.5;
                // gap between columns
                final int betweenGap = (int) (width / (addressBookMetadata.getNumColumns() - 1 + 2 * marginFraction));
                // gap between leftmost column and left edge (and similar on right)
                final int sideGap = (int) (betweenGap * marginFraction);

                // find the column for e2 next to the column for e1
                final int xPos = sideGap + addressBookMetadata.mems2col(e1, e2) * betweenGap;

                return xPos;
    }

    /**
     * find y position on the screen for an event
     */
    public int ypos(final EventImpl event) {
//        if (event.getBaseEvent().getBranchIndex() == -1 || !branchIndexToY.containsKey(event.getBaseEvent().getBranchIndex())) {
            return (event == null) ? -100 : (int) (ymax - r * (1 + 2 * (event.getGeneration() - minGen)));
//        } else {
//            return branchIndexToY.get(event.getBaseEvent().getBranchIndex());
//            return (event == null) ? -100 : (int) (ymax - r * (1 + 2 * (event.getGeneration() - event.getBaseEvent().getBranchIndex() - minGen)));
//        }
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
