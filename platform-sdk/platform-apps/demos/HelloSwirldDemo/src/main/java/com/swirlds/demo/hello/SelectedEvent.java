package com.swirlds.demo.hello;

import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.input.MouseEvent;
import javafx.scene.shape.Sphere;

public record SelectedEvent(GuiEvent event, java.util.List<Sphere> parents, Sphere circle, SelectedEventManager manager, Group circleGroup, Group lineGroup) implements EventHandler<MouseEvent> {

    @Override
    public void handle(final MouseEvent mouseEvent) {

        manager.eventSelected(this);
        circleGroup.toFront();
        lineGroup.toBack();
    }
}
