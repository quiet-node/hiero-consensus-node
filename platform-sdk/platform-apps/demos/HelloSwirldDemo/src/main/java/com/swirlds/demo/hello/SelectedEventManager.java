package com.swirlds.demo.hello;

public class SelectedEventManager {

    private SelectedEvent currentEvent;

    public void eventSelected(final SelectedEvent selectedEvent){
        if(currentEvent != null){
            currentEvent.circle().setRadius(20);
        }
        selectedEvent.circle().setRadius(25);
        currentEvent = selectedEvent;
    }
}
