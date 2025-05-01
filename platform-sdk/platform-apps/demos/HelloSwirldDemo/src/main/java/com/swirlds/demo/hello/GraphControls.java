package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.RosterEntry;
import java.util.Timer;
import java.util.TimerTask;
import javafx.application.Platform;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

public class GraphControls {

    private GuiGraphGenerator graphGenerator;
    private GraphPane graphPane;
    private Timer timer;
    private TimerTask producer;

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
        var leftPane = new Label("Left pane\n" + String.join(",",
                roster.rosterEntries().stream().map(e -> Long.toString(e.nodeId())).toList()));
        leftPane.setMinWidth(200);
        hBox.getChildren().add(leftPane);
        hBox.getChildren().add(vBox);

        this.timer = new Timer(true);
        return hBox;

    }

    private Node createBottomPane() {
        var hBox = new HBox();
        var nextEvent = new Button();
        nextEvent.setGraphic(new ImageView("/right.png"));
        hBox.getChildren().add(nextEvent);

        nextEvent.setOnAction(event -> {
            generateSingleEvent();
        });
        var spacer = new Region();
        spacer.setPrefWidth(50);
        hBox.getChildren().add(spacer);

        var pauseGenerator = new Button();
        var runGenerator = new Button();

        pauseGenerator.setGraphic(new ImageView("/pause.png"));
        pauseGenerator.setOnAction(event -> {
            if ( producer != null ) {
                producer.cancel();
                producer = null;
                runGenerator.setDisable(false);
                pauseGenerator.setDisable(true);
            }
        });
        pauseGenerator.setDisable(true);
        hBox.getChildren().add(pauseGenerator);

        runGenerator.setGraphic(new ImageView("/play.png"));
        runGenerator.setOnAction(event -> {
            if (producer != null) {
                return;
            }
            this.producer = new TimerTask() {
                @Override
                public void run() {
                    generateSingleEvent();
                }
            };
            timer.scheduleAtFixedRate(producer, 250, 250);
            runGenerator.setDisable(true);
            pauseGenerator.setDisable(false);
        });
        hBox.getChildren().add(runGenerator);

        return hBox;

    }

    private void generateSingleEvent() {
        var events = graphGenerator.generateEvents(1);
        Platform.runLater(() -> {
            for (GuiEvent guiEvent : events) {
                graphPane.addEventNode(guiEvent, (int) (Math.random() * 500), (int) (Math.random() * 500));
            }
        });
    }


}
