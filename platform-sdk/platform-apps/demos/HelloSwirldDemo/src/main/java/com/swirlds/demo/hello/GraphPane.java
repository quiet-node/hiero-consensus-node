package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.Roster;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.scene.paint.PhongMaterial;
import javafx.scene.shape.Circle;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import javafx.scene.shape.Line;
import javafx.scene.shape.Sphere;
import javafx.scene.text.Text;

import java.util.HashMap;
import java.util.Map;

public class GraphPane extends Pane {
    private Map<Bytes, GuiEvent> nodeViews = new HashMap<>();
    private Map<Long, Integer> nodePositions = new HashMap<Long, Integer>();
    private final int paneWidth = 900;
    private final int paneHeight = 900;
    private final int circleRadius = 20;

    public void setup(Roster roster) {
        this.setStyle("-fx-background-color: grey;");
        this.setPrefSize(paneWidth,paneHeight);

        // add logic based on number of nodes
        setupTimeLines(roster);
        // step 1 get roster size - use to determine number of vertical timelines
        // step 2 get node id - use to determine horizontal position
        // can use creator id and generation to determine horizontal position
    }

    public void addEventNode(GuiEvent event) {
        final long nodePos = nodePositions.get(event.creator());
        final long generationOffset = paneHeight - (event.generation() * circleRadius * 2) - (circleRadius * 2);

        Sphere circle = new Sphere(circleRadius);
        var material = new PhongMaterial(event.color());
        material.setSpecularColor(Color.WHITE);
        circle.setMaterial(material);

        circle.relocate(nodePos, generationOffset);
        Text text = new Text(event.generation() + " - " + event.creator());
        text.relocate(nodePos - 40, generationOffset + 10);
        this.getChildren().addAll(circle, text);
        nodeViews.put(event.id(), event);
    }

    private void setupTimeLines(Roster roster) {
        int numNodes = roster.rosterEntries().size();
        int xOffset = paneWidth / (numNodes + 1);

        for (int i = 0; i < numNodes; i++) {
            var node = roster.rosterEntries().get(i);
            var nodeId = node.nodeId();
            var xPos = (i + 1) * xOffset;
            nodePositions.put(nodeId, xPos - circleRadius);
            var yPos = 0;
            var line = new Line(xPos, yPos, xPos, paneHeight);
            line.setStroke(Color.RED);
            this.getChildren().add(line);

            Text text = new Text(Long.toString(nodeId));
            text.relocate(xPos - 10, yPos + 10);
            this.getChildren().add(text);
        }
    }
}
