package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.Roster;
import javafx.scene.Group;
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
    private Group nodeGroup;
    public int maxEventHeight = 0; // note this is a negative value but visual the top

    public void setup(Roster roster) {
        this.setStyle("-fx-background-color: grey;");
        this.setPrefSize(paneWidth,paneHeight);

        // add logic based on roster to set number of timelines
        setupTimeLines(roster);

        nodeGroup = new Group();
        this.getChildren().add(nodeGroup);
    }

    public void addEventNode(GuiEvent event) {
        final long nodePos = nodePositions.get(event.creator());
        final long generationOffset = -(event.generation() * circleRadius * 2) - (circleRadius * 2);

        Sphere circle = new Sphere(circleRadius);
        var material = new PhongMaterial(event.color());
        material.setSpecularColor(Color.WHITE);
        circle.setMaterial(material);

        circle.relocate(nodePos, generationOffset);
        Text text = new Text(event.generation() + " - " + event.creator());
        text.relocate(nodePos - 40, generationOffset + 10);
        nodeGroup.getChildren().addAll(circle, text);
        nodeViews.put(event.id(), event);

        System.out.println("eventId: " + event.id().toString() + ", gen: " + event.generation() + ", nodePos: " + nodePos + ", generationOffset: " + generationOffset);

        if (generationOffset < maxEventHeight) {
            System.out.println("maxEventHeight: " + maxEventHeight + ", prefHeight: " + maxEventHeight + paneHeight);
            maxEventHeight = (int) generationOffset;
        }
    }

    public int getMaxEventHeight() {
        return maxEventHeight;
    }

    public void setGroupYTranslation(int yTranslation) {
        nodeGroup.setTranslateY(yTranslation);
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
