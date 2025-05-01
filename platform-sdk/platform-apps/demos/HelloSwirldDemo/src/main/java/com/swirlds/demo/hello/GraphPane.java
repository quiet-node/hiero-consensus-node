package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.Roster;
import javafx.animation.ScaleTransition;
import javafx.geometry.Point2D;
import javafx.scene.Group;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.scene.paint.PhongMaterial;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import javafx.scene.shape.Line;
import javafx.scene.shape.Sphere;
import javafx.scene.text.Text;
import javafx.scene.transform.Translate;

import java.util.HashMap;
import java.util.Map;
import javafx.util.Duration;

public class GraphPane extends Pane {
    private Map<Bytes, GuiEVentGroupMember> nodeViews = new HashMap<>();
    private Map<Long, Integer> nodePositions = new HashMap<Long, Integer>();
    private final int paneWidth = 900;
    private final int paneHeight = 1000;
    private final int circleRadius = 20;
    private final int circleDiameter = circleRadius * 2;
    private Group nodeGroup;
    public int maxEventHeight = circleRadius * 4;
    private int totalHeight = paneHeight;
    private Translate groupTranslation;
    private int cumulativeYTranslation = 0;
    private final SelectedEventManager selectedEventManager = new SelectedEventManager();

    public void setup(Roster roster) {
        this.setStyle("-fx-background-color: grey;");
        this.setPrefSize(paneWidth,paneHeight);

        // add logic based on roster to set number of timelines
        setupTimeLines(roster);

        nodeGroup = new Group();

        // setup translation logic to move a diameter at a time
        groupTranslation = new Translate();
        groupTranslation.setY(circleDiameter);

        this.getChildren().add(nodeGroup);
    }

    private Point2D getEventPosition(GuiEvent event) {
        // get the position of the event based on its generation and node id
        var nodePos = nodePositions.get(event.creator());
        var generationOffset = paneHeight - (event.generation() * circleDiameter) - (circleDiameter * 2);
        return new Point2D(nodePos, generationOffset);
    }

    public void addEventNode(GuiEvent event) {
        final Point2D eventPosition = getEventPosition(event);
        
        var material = new PhongMaterial(event.color());
        material.setSpecularColor(Color.WHITE);
        material.setSpecularPower(4);

        Sphere circle;
        boolean isNewEvent = !nodeViews.containsKey(event.id());
        circle = isNewEvent ? new Sphere(circleRadius) : (Sphere) nodeGroup.getChildren().get(nodeViews.get(event.id()).groupPosition());
        circle.setMaterial(material);

        // can remove if not needed
        circle.setUserData(event);
        circle.setOnMouseClicked(new SelectedEvent(event, circle, selectedEventManager));

        if (isNewEvent) {
            circle.relocate(eventPosition.getX(), eventPosition.getY());

            nodeGroup.getChildren().add(circle);
            nodeViews.put(event.id(), new GuiEVentGroupMember(event, nodeGroup.getChildren().size() - 1));

            // add metadata text to the event
//            Text text = new Text(event.generation() + " - " + event.creator() + " - " + event.birthRound() + " - " + event.votingRound());
//            text.relocate(eventPosition.getX() - 40, eventPosition.getY() + 10);
//            nodeGroup.getChildren().add(text);

            addParentEdge(event);
        }

        // add gen label
        Text genText = new Text("Gen " + event.generation() + ":");
        // add gen label if note present
        if (!nodeGroup.getChildren().contains(genText)) {
            genText.relocate(0, eventPosition.getY() + 10);
            nodeGroup.getChildren().add(genText);
        }

        // update max height when events exceed original window height
        if (eventPosition.getY() < maxEventHeight) {
            maxEventHeight = (int) eventPosition.getY();
            totalHeight += circleDiameter;
            cumulativeYTranslation += circleDiameter;

            //setGroupYTranslation(cumulativeYTranslation);
        }

        ScaleTransition st = new ScaleTransition(Duration.millis(250), circle);
        st.setByX(1.5f);
        st.setByY(1.5f);
        st.setCycleCount(2);
        st.setAutoReverse(true);

        st.play();
    }

    @Deprecated
    public int getMaxEventHeight() {
        return maxEventHeight;
    }

    public int getTotalHeight() {
        return totalHeight;
    }

    public void setGroupYTranslation(int yTranslation) {
        // cycle transform - temp solution need to come back
        nodeGroup.getTransforms().remove(groupTranslation);

        groupTranslation.setY(yTranslation);
        nodeGroup.getTransforms().add(groupTranslation);
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

            Text text = new Text("Node " + Long.toString(nodeId) + ":");
            text.relocate(xPos - 45, yPos + 10);
            this.getChildren().add(text);
        }
    }

    private void addParentEdge(GuiEvent event) {
        // add a line from event to its ancestor parent
        for (var parent : event.parents()) {
            var parentEvent = nodeViews.get(parent).event();
            if (parentEvent != null) {
                final Point2D parentEventPosition = getEventPosition(parentEvent);
                final Point2D eventPosition = getEventPosition(event);
                var line = new Line(
                        eventPosition.getX() + circleRadius,
                        eventPosition.getY() + circleRadius,
                        parentEventPosition.getX() + circleRadius,
                        parentEventPosition.getY() + circleRadius);
                line.setStroke(Color.BLUE); // should we use a different colour for self parent?
                nodeGroup.getChildren().add(line);
            }
        }
    }
}

record GuiEVentGroupMember(
        GuiEvent event,
        int groupPosition
) {}
