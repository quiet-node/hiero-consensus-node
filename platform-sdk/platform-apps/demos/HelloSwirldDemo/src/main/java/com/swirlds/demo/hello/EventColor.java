package com.swirlds.demo.hello;

import javafx.scene.paint.Color;

public class EventColor {
    /** unknown-fame witness, non-consensus */
    public static final Color LIGHT_RED = Color.color(0.6, 0, 0);
    /** unknown-fame witness, consensus (which can't happen) */
    public static final Color DARK_RED = Color.color(0.25, 0, 0);
    /** famous witness, non-consensus */
    public static final Color LIGHT_GREEN = Color.color(0, 0.6, 0);
    /** famous witness, consensus */
    public static final Color DARK_GREEN = Color.color(0, 0.25, 0);
    /** non-famous witness, non-consensus */
    public static final Color LIGHT_YELLOW = Color.color(0.5, 0.5, 0);
    /** non-famous witness, consensus */
    public static final Color DARK_YELLOW = Color.color(0.2, 0.2, 0);
    /** judge, consensus */
    public static final Color LIGHT_BLUE = Color.color(0, 0, 0.6);
    /** judge, non-consensus */
    public static final Color DARK_BLUE = Color.color(0, 0, 0.25);
    /** non-witness, consensus */
    public static final Color LIGHT_GRAY = Color.color(0.3, 0.3, 0.3);
    /** non-witness, non-consensus */
    public static final Color DARK_GRAY = Color.color(0, 0, 0);

    /**
     * Return the color for an event based on calculations in the consensus algorithm A non-witness is gray,
     * and a witness has a color of green (famous), blue (not famous) or red (undecided fame). When the
     * event becomes part of the consensus, its color becomes darker.
     *
     * @param event
     * 		the event to color
     * @return its color
     */
    public static Color eventColor(final GuiEvent event) {
        if (!event.witness()) {
            return event.consensus() ? DARK_GRAY : LIGHT_GRAY;
        }
        // after this point, we know the event is a witness
        if (!event.decided()) {
            return event.consensus() ? DARK_RED : LIGHT_RED;
        }
        // after this point, we know the event is a witness and fame is decided
        if (event.judge()) {
            return event.consensus() ? DARK_BLUE : LIGHT_BLUE;
        }
        if (event.famous()) {
            return event.consensus() ? DARK_GREEN : LIGHT_GREEN;
        }

        // if we reached here, it means the event is a witness, fame is decided, but it is not famous
        return event.consensus() ? DARK_YELLOW : LIGHT_YELLOW;
    }
}
