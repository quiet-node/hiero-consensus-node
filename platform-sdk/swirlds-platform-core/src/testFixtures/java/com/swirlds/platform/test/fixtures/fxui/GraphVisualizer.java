package com.swirlds.platform.test.fixtures.fxui;

import java.util.List;

public interface GraphVisualizer {
    default void displayEvent(final GuiEvent event){
        displayEvents(List.of(event));
    }
    void displayEvents(final List<GuiEvent> event);
}
