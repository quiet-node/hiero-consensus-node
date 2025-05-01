package com.swirlds.demo.hello;

import javafx.scene.paint.Color;

public class EventColor {
    private static final double C5 = 1.00;
    private static final double C4 = 0.80;
    private static final double C3 = 0.50;
    private static final double C2 = 0.20;
    private static final double C1 = 0.00;


    /** unknown-fame witness, non-consensus */
    public static final Color LIGHT_RED = Color.color(C4, C1, C1);
    /** unknown-fame witness, consensus (which can't happen) */
    public static final Color DARK_RED = Color.color(C2, C1, C1);
    /** famous witness, non-consensus */
    public static final Color LIGHT_GREEN = Color.color(C1, C4, C1);
    /** famous witness, consensus */
    public static final Color DARK_GREEN = Color.color(C1, C2, C1);
    /** non-famous witness, non-consensus */
    public static final Color LIGHT_YELLOW = Color.color(C4, C4, C1);
    /** non-famous witness, consensus */
    public static final Color DARK_YELLOW = Color.color(C2, C2, C1);
    /** judge, consensus */
    public static final Color LIGHT_BLUE = Color.color(C1, C1, C4);
    /** judge, non-consensus */
    public static final Color DARK_BLUE = Color.color(C1, C1, C2);
    /** non-witness, consensus */
    public static final Color LIGHT_GRAY = Color.color(C4, C4, C4);
    /** non-witness, non-consensus */
    public static final Color DARK_GRAY = Color.color(C1, C1, C1);

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
