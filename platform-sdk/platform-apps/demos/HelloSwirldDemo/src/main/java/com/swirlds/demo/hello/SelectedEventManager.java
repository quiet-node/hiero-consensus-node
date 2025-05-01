package com.swirlds.demo.hello;

public class SelectedEventManager {

    private SelectedEvent currentEvent;

    public void eventSelected(final SelectedEvent selectedEvent){
        System.out.println("Selected event: " + selectedEvent.event());
        if(currentEvent != null){
            currentEvent.circle().setRadius(Constants.CIRCLE_RADIUS);
        }
        selectedEvent.circle().setRadius(Constants.SELECTED_CIRCLE_RADIUS);
        currentEvent = selectedEvent;
    }
}
