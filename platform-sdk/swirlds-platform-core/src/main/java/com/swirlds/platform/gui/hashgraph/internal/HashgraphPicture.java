// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui.hashgraph.internal;

import static com.swirlds.logging.legacy.LogMarker.EXCEPTION;
import static com.swirlds.platform.gui.hashgraph.HashgraphGuiConstants.HASHGRAPH_PICTURE_FONT;

import com.swirlds.platform.Consensus;
import com.swirlds.platform.consensus.CandidateWitness;
import com.swirlds.platform.gui.hashgraph.HashgraphGuiConstants;
import com.swirlds.platform.gui.hashgraph.HashgraphGuiSource;
import com.swirlds.platform.gui.hashgraph.HashgraphPictureOptions;
import com.swirlds.platform.internal.EventImpl;
import com.swirlds.platform.system.address.AddressBook;
import java.awt.AWTException;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Rectangle;
import java.awt.Robot;
import java.awt.event.ItemEvent;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.Serial;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.JPanel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.consensus.model.event.EventConstants;

/**
 * This panel has the hashgraph picture, and appears in the window to the right of all the settings.
 */
public class HashgraphPicture extends JPanel {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LogManager.getLogger(HashgraphPicture.class);
    private final HashgraphGuiSource hashgraphSource;
    private final HashgraphPictureOptions options;
    private final EventSelector selector;
    private PictureMetadata pictureMetadata;
    /** used to store an image when the freeze checkbox is checked */
    private BufferedImage image = null;

    private AddressBookMetadata nonExpandedMetadata;
    private AddressBookMetadata expandedMetadata;
//    private Set<EventDescriptorWrapper> selfParents = new HashSet<>();
//    private Map<EventDescriptorWrapper, >
//    private Map<EventImpl, Long> eventCount = new HashMap<>();
//    private Map<Integer, Integer> xToY = new HashMap<>();
    private List<EventImpl> events = new ArrayList<>();
    int refreshCounter = 0;
    Map<EventImpl, Integer> eventFirstOccurence = new HashMap<>();
    Map<Long, List<EventImpl>> branchIndexToEvents = new HashMap<>();
    Map<Long, Integer> branchIndexToY = new HashMap<>();
    Map<Long, Integer> branchIndexToX = new HashMap<>();
    private final Map<EventImpl, Integer> eventToX = new HashMap<>();

    public HashgraphPicture(final HashgraphGuiSource hashgraphSource, final HashgraphPictureOptions options) {
        this.hashgraphSource = hashgraphSource;
        this.options = options;
        this.selector = new EventSelector();
        this.addMouseListener(selector);
        createMetadata();
    }

    private void createMetadata() {
        if ((expandedMetadata == null || nonExpandedMetadata == null) && hashgraphSource.isReady()) {
            expandedMetadata = new AddressBookMetadata(hashgraphSource.getAddressBook(), true);
            nonExpandedMetadata = new AddressBookMetadata(hashgraphSource.getAddressBook(), false);
        }
    }

    @Override
    public void paintComponent(final Graphics g) {
        super.paintComponent(g);
        try {
            if (image != null) {
                g.drawImage(image, 0, 0, null);
                return;
            }
            if (!hashgraphSource.isReady()) {
                return;
            }
            createMetadata();
            g.setFont(HASHGRAPH_PICTURE_FONT);
            final FontMetrics fm = g.getFontMetrics();
            final AddressBook addressBook = hashgraphSource.getAddressBook();
            final int numMem = addressBook.getSize();
            final AddressBookMetadata currentMetadata = options.isExpanded() ? expandedMetadata : nonExpandedMetadata;

            List<EventImpl> events;
            if (options.displayLatestEvents()) {
                final long startGen = Math.max(
                        hashgraphSource.getMaxGeneration() - options.getNumGenerationsDisplay() + 1,
                        EventConstants.FIRST_GENERATION);
                options.setStartGeneration(startGen);
                events = hashgraphSource.getEvents(startGen, options.getNumGenerationsDisplay());
            } else {
                events = hashgraphSource.getEvents(options.getStartGeneration(), options.getNumGenerationsDisplay());
            }
            // in case the state has events from creators that don't exist, don't show them
            if (events == null) { // in case a screen refresh happens before any events
                return;
            }
            events = events.stream()
                    .filter(e -> addressBook.contains(e.getCreatorId()))
                    .filter(e -> addressBook.getIndexOfNodeId(e.getCreatorId()) < numMem)
                    .toList();

            pictureMetadata = new PictureMetadata(fm, this.getSize(), currentMetadata, events, branchIndexToX, branchIndexToY, eventToX);

            selector.setMetadata(pictureMetadata);
            selector.setEventsInPicture(events);

            g.setColor(Color.BLACK);

            for (int i = 0; i < currentMetadata.getNumColumns(); i++) {
                final String name = currentMetadata.getLabel(i);

                // gap between columns
                final int betweenGap = pictureMetadata.getGapBetweenColumns();
                // gap between leftmost column and left edge (and similar on right)
                final int sideGap = pictureMetadata.getSideGap();
                final int x = sideGap + (i) * betweenGap;
                g.drawLine(x, pictureMetadata.getYmin(), x, pictureMetadata.getYmax());
                final Rectangle2D rect = fm.getStringBounds(name, g);
                g.drawString(
                        name, (int) (x - rect.getWidth() / 2), (int) (pictureMetadata.getYmax() + rect.getHeight()));
            }

            final int d = pictureMetadata.getD();

            // for each event, draw 2 downward lines to its parents
            for (final EventImpl event : events) {
                drawLinksToParents(g, event);
            }

            // for each event, draw its circle
            for (final EventImpl event : events) {
                drawEventCircle(g, event, options, d);
            }
            refreshCounter++;
        } catch (final Exception e) {
            logger.error(EXCEPTION.getMarker(), "error while painting", e);
        }
    }

    private void drawLinksToParents(final Graphics g, final EventImpl event) {
        g.setColor(HashgraphGuiUtils.eventColor(event, options));
        final EventImpl e1 = event.getSelfParent();
        EventImpl e2 = event.getOtherParent();
        final AddressBook addressBook = hashgraphSource.getAddressBook();
        if (e2 != null
                && (!addressBook.contains(e2.getCreatorId())
                        || addressBook.getIndexOfNodeId(e2.getCreatorId()) >= addressBook.getSize())) {
            // if the creator of the other parent has been removed,
            // treat it as if there is no other parent
            e2 = null;
        }
        if (e1 != null && e1.getGeneration() >= pictureMetadata.getMinGen()) {
            g.drawLine(
                    pictureMetadata.xpos(e2, event, false),
                    pictureMetadata.ypos(event),
                    pictureMetadata.xpos(e2, event, false),
                    pictureMetadata.ypos(e1));

//                branchIndexToX.put(event.getBaseEvent().getBranchIndex(), pictureMetadata.xpos(e2, event, false) + 10);
//                final var lastX = branchIndexToX.get(event.getBaseEvent().getBranchIndex());
//                eventToX.put(event, lastX + 10);
//                branchIndexToY.put(event.getBaseEvent().getBranchIndex(), pictureMetadata.ypos(event));
        }
        if (e2 != null && e2.getGeneration() >= pictureMetadata.getMinGen()) {
            g.drawLine(
                    pictureMetadata.xpos(e2, event, false),
                    pictureMetadata.ypos(event),
                    pictureMetadata.xpos(event, e2, false),
                    pictureMetadata.ypos(e2));

//                branchIndexToX.put(event.getBaseEvent().getBranchIndex(), pictureMetadata.xpos(event, e2, false) + 10);
//                final var lastX = branchIndexToX.get(event.getBaseEvent().getBranchIndex());
//                eventToX.put(event, lastX + 10);
//                branchIndexToY.put(event.getBaseEvent().getBranchIndex(), pictureMetadata.ypos(e2));
        }
    }

    private void drawEventCircle(
            final Graphics g, final EventImpl event, final HashgraphPictureOptions options, final int d) {
        final Consensus consensus = hashgraphSource.getEventStorage().getConsensus();
        final FontMetrics fm = g.getFontMetrics();
        final int fa = fm.getMaxAscent();
        final int fd = fm.getMaxDescent();
        final EventImpl e2 = event.getOtherParent() != null
                        && hashgraphSource
                                .getAddressBook()
                                .contains(event.getOtherParent().getCreatorId())
                ? event.getOtherParent()
                : null;
        final Color color;
        if (selector.isSelected(event)) {
            color = Color.MAGENTA;
        } else if (selector.isStronglySeen(event)) {
            color = Color.CYAN;
        } else {
            color = HashgraphGuiUtils.eventColor(event, options);
        }
        g.setColor(color);

        final int xPos = pictureMetadata.xpos(e2, event, false) - d / 2;
        final int yPos = pictureMetadata.ypos(event) - d / 2;

        if(branchIndexToX.containsKey(event.getBaseEvent().getBranchIndex())) {
            branchIndexToX.put(event.getBaseEvent().getBranchIndex(), xPos);

            final var lastX = branchIndexToX.get(event.getBaseEvent().getBranchIndex());

            eventToX.put(event, lastX);
        } else if (event.getBaseEvent().getBranchIndex() != -1) {
            eventToX.put(event, xPos);
            branchIndexToX.put(event.getBaseEvent().getBranchIndex(), xPos);
        }

        branchIndexToY.put(event.getBaseEvent().getBranchIndex(), yPos);

        eventFirstOccurence.put(event, refreshCounter);

        if(event.getBaseEvent().getBranchIndex() != -1L){
            final var newXPos = eventToX.get(event);
            final var newYPos = branchIndexToY.get(event.getBaseEvent().getBranchIndex());
            g.fillOval(newXPos, newYPos, d, d);
        } else {
            g.fillOval(xPos, yPos, d, d);
        }

        events.add(event);

        g.setFont(g.getFont().deriveFont(Font.BOLD));

        String s = "";

        if (options.writeRoundCreated()) {
            s += " " + event.getRoundCreated();
        }
        if (options.writeVote() && event.isWitness()) {
            for (final Iterator<CandidateWitness> it =
                            consensus.getRounds().getElectionRound().undecidedWitnesses();
                    it.hasNext(); ) {
                final CandidateWitness candidateWitnessI = it.next();
                String vote = event.getVote(candidateWitnessI) ? "T" : "F";
                // showing T or F from true/false for readability on the picture
                s += vote
                        // showing first two characters from the hash of the witness
                        // current event is voting on(example H:aa)
                        + candidateWitnessI.getWitness().shortString().substring(5, 10) + "|";
            }
        }
        if (options.writeEventHash()) {
            // showing first two characters from the hash of the event
            s += " h:" + event.getBaseHash().toString().substring(0, 2);
        }
        if (options.writeRoundReceived() && event.getRoundReceived() > 0) {
            s += " " + event.getRoundReceived();
        }
        // if not consensus, then there's no order yet
        if (options.writeConsensusOrder() && event.isConsensus()) {
            s += " " + event.getBaseEvent().getConsensusOrder();
        }
        if (options.writeConsensusTimeStamp()) {
            final Instant t = event.getConsensusTimestamp();
            if (t != null) {
                s += " " + HashgraphGuiConstants.FORMATTER.format(t);
            }
        }
        if (options.writeGeneration()) {
            s += " " + event.getGeneration();
        }

        if (options.writeBirthRound()) {
            s += " " + event.getBirthRound();
        }

        if (options.showBranches() && hashgraphSource.getEventStorage().getBranchIndexMap().containsKey(event.getBaseEvent().getGossipEvent()) &&
                !hashgraphSource.getEventStorage().getIsSingleEventInBranchMap().get(event.getBaseEvent().getGossipEvent())) {
            s += " " + "branch " + hashgraphSource.getEventStorage().getBranchIndexMap().get(event.getBaseEvent().getGossipEvent());
        }

        if (!s.isEmpty()) {
            final Rectangle2D rect = fm.getStringBounds(s, g);
            final int x = (int) (pictureMetadata.xpos(e2, event, true) - rect.getWidth() / 2. - fa / 4.);
            final int y = (int) (pictureMetadata.ypos(event) + rect.getHeight() / 2. - fd / 2);
            g.setColor(HashgraphGuiConstants.LABEL_OUTLINE);
            g.drawString(s, x - 1, y - 1);
            g.drawString(s, x + 1, y - 1);
            g.drawString(s, x - 1, y + 1);
            g.drawString(s, x + 1, y + 1);
            g.setColor(color);
            g.drawString(s, x, y);
        }
    }

    public void freezeChanged(final ItemEvent e) {
        if (e.getStateChange() == ItemEvent.SELECTED) {
            try { // capture a bitmap of "picture" from the screen
                image = (new Robot())
                        .createScreenCapture(new Rectangle(
                                this.getLocationOnScreen(),
                                this.getVisibleRect().getSize()));
                // to write the image to disk:
                // ImageIO.write(image, "jpg", new File("image.jpg"));
            } catch (final AWTException err) {
                // ignore exception
            }
        } else if (e.getStateChange() == ItemEvent.DESELECTED) {
            image = null; // erase the saved image, stop freezing
        }
    }
}
