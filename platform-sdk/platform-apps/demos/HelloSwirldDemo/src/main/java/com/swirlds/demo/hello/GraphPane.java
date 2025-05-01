package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.Roster;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import javafx.scene.text.Text;

import java.util.HashMap;
import java.util.Map;

public class GraphPane extends Pane {
    private Map<Bytes, GuiEvent> nodeViews = new HashMap<>();

    public void setup(Roster roster) {
        this.setStyle("-fx-background-color: black;");
        this.setPrefSize(900,900);

        // add logic based on number of nodes
    }

    public void addEventNode(GuiEvent event, int xOffset, int yOffset) {
        Circle circle = new Circle(50, Color.BLUE);
        circle.relocate(20 + xOffset, 20 + yOffset);
        Text text = new Text(event.generation() + " - " + event.creator());
        text.relocate(20 + xOffset, 20 + yOffset);
        this.getChildren().addAll(circle, text);
        nodeViews.put(event.id(), event);
    }
}
