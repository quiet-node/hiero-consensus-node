package com.swirlds.demo.hello;

public class SelectedEventManager {

    private SelectedEvent currentEvent;

    public void eventSelected(final SelectedEvent selectedEvent){
        if(currentEvent != null){
            currentEvent.circle().setRadius(Constants.CIRCLE_RADIUS);
        }
        selectedEvent.circle().setRadius(Constants.SELECTED_CIRCLE_RADIUS);
        currentEvent = selectedEvent;
    }
}
