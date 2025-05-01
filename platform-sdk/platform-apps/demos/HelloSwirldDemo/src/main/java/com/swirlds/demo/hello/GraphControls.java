package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.RosterEntry;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;

public class GraphControls {

    public Pane setup(GuiGraphGenerator graphGenerator) {
        var roster = graphGenerator.getRoster();
        GraphPane graphPane = new GraphPane();
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
        var button = new Button();
        button.setGraphic(new ImageView("/left.png"));
        button.maxHeight(200);
        hBox.getChildren().add(button);
        hBox.minHeight(300);
        return hBox;

    }


}
