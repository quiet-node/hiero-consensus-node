package com.swirlds.demo.hello;

import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.layout.Pane;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Rectangle;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import javafx.scene.text.Text;

import java.util.HashMap;
import java.util.Map;

public class GraphPane extends Pane {
    private Map<Bytes, GuiEvent> nodeViews = new HashMap<>();

    public void setup() {
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
