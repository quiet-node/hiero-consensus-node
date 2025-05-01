package com.swirlds.demo.hello;

import javafx.scene.control.Label;

public class SelectedEventManager {
    private final Label selectedLabel;

    public SelectedEventManager(final Label selectedLabel) {
        this.selectedLabel = selectedLabel;
    }

    private SelectedEvent currentEvent;

    public void eventSelected(final SelectedEvent selectedEvent){
        System.out.println("Selected event: " + selectedEvent.event());
        if(currentEvent != null){
            currentEvent.circle().setRadius(Constants.CIRCLE_RADIUS);
        }
        selectedEvent.circle().setRadius(Constants.SELECTED_CIRCLE_RADIUS);
        currentEvent = selectedEvent;
        selectedLabel.setText(selectedEvent.event().toString());
    }
}
