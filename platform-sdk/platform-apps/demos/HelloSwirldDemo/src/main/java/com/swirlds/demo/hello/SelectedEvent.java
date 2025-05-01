package com.swirlds.demo.hello;

import javafx.event.EventHandler;
import javafx.scene.input.MouseEvent;
import javafx.scene.shape.Sphere;

public record SelectedEvent(GuiEvent event, Sphere circle, SelectedEventManager manager) implements EventHandler<MouseEvent> {

    @Override
    public void handle(final MouseEvent mouseEvent) {

        manager.eventSelected(this);
    }
}
