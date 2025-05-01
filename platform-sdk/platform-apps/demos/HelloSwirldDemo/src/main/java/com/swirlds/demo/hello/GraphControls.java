package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.RosterEntry;
import javafx.application.Platform;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;

public class GraphControls {

    private GuiGraphGenerator graphGenerator;
    private GraphPane graphPane;

    public Pane setup(GuiGraphGenerator graphGenerator) {
        this.graphGenerator = graphGenerator;
        var roster = graphGenerator.getRoster();
        this.graphPane = new GraphPane();
        graphPane.setup(roster);
        VBox vBox = new VBox();
        ScrollPane scrollPane = new ScrollPane(graphPane);
        vBox.getChildren().add(scrollPane);
        vBox.getChildren().add(createBottomPane());

        HBox hBox = new HBox();
        var leftPane = new Label("Left pane\n"+String.join(",",
                roster.rosterEntries().stream().map(e -> Long.toString(e.nodeId())).toList()));
        leftPane.setMinWidth(200);
        hBox.getChildren().add(leftPane);
        hBox.getChildren().add(vBox);
        return hBox;

    }

    private Node createBottomPane() {
        var hBox = new HBox();
        var nextEvent = new Button();
        nextEvent.setGraphic(new ImageView("/right.png"));
        hBox.getChildren().add(nextEvent);

        nextEvent.setOnAction(event -> {
           var events = graphGenerator.generateEvents(1);
           Platform.runLater(() -> {
               for (GuiEvent guiEvent : events) {
                   graphPane.addEventNode(guiEvent, (int) (Math.random()*500), (int) (Math.random()*500));
               }
           });

        });


        return hBox;

    }


}
