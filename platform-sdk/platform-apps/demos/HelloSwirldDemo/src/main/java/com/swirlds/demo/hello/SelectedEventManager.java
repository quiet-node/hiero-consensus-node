package com.swirlds.demo.hello;

import javafx.scene.control.Label;

public class SelectedEventManager {
    private final Label selectedLabel;

    public SelectedEventManager(final Label selectedLabel) {
        this.selectedLabel = selectedLabel;
    }

    private SelectedEvent currentEvent;

    public void eventSelected(final SelectedEvent selectedEvent){
        if(currentEvent != null){
            currentEvent.circle().setRadius(Constants.CIRCLE_RADIUS);
            currentEvent.parents().forEach(parent -> {parent.setRadius(Constants.CIRCLE_RADIUS);});
            if(currentEvent.event().id().equals(selectedEvent.event().id())){
                // deselect the event
                selectedLabel.setText("No event selected");
                currentEvent =null;
                return;
            }
        }

        selectedEvent.circle().setRadius(Constants.SELECTED_CIRCLE_RADIUS);
        selectedEvent.parents().forEach(parent -> {parent.setRadius(Constants.SELECTED_PARENT_RADIUS);});
        currentEvent = selectedEvent;
        selectedLabel.setText(
                "Selected event:\n" +
                selectedEvent.event().toString()
        );
    }
}
