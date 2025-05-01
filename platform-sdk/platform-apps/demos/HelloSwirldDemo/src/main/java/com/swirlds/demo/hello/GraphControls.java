package com.swirlds.demo.hello;

import com.hedera.hapi.node.state.roster.RosterEntry;
import java.util.Timer;
import java.util.TimerTask;
import javafx.application.Platform;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollBar;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Slider;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Border;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.BorderStroke;
import javafx.scene.layout.BorderStrokeStyle;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.paint.PhongMaterial;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Rectangle;
import javafx.scene.shape.Sphere;

public class GraphControls {

    private GuiGraphGenerator graphGenerator;
    private GraphPane graphPane;
    private Timer timer;
    private TimerTask producer;
    private ScrollBar scrollBar;

    public Pane setup(GuiGraphGenerator graphGenerator) {
        this.graphGenerator = graphGenerator;
        var roster = graphGenerator.getRoster();
        this.graphPane = new GraphPane();
        graphPane.setup(roster);

        HBox hBox = new HBox();
        var leftPane = new VBox(15);
        fillLeftPane(leftPane);
        this.scrollBar = new ScrollBar();
        scrollBar.setOrientation(Orientation.VERTICAL);


        hBox.getChildren().add(leftPane);
        hBox.getChildren().add(graphPane);
        hBox.getChildren().add(scrollBar);

        scrollBar.valueProperty().addListener((observable, oldValue, newValue) -> {
            System.out.println("ScrollBar value: " + newValue);
        });


        this.timer = new Timer(true);
        return hBox;

    }

    private void fillLeftPane(VBox leftPane) {

        addBubbleEvent(leftPane, Color.LIGHTBLUE,Color.DARKBLUE, "Something");
        addBubbleEvent(leftPane, Color.LIGHTGREEN, Color.DARKGREEN, "Something else");
        Rectangle spacer = new Rectangle();
        spacer.minHeight(500);
        leftPane.getChildren().add(spacer);
        leftPane.getChildren().add(createBottomPane());
    }

    private void addBubbleEvent(VBox leftPane, Color color, Color secondColor, String text) {
        HBox hBox = new HBox(5);
        {
            Sphere circle = new Sphere(20);
            var material = new PhongMaterial(color);
            material.setSpecularColor(Color.WHITE);
            material.setSpecularPower(4);
            circle.setMaterial(material);

            hBox.getChildren().add(circle);
        }

        {
            Sphere circle = new Sphere(20);
            var material = new PhongMaterial(secondColor);
            material.setSpecularColor(Color.WHITE);
            material.setSpecularPower(4);
            circle.setMaterial(material);

            hBox.getChildren().add(circle);
        }
        var label = new Label(text);
        hBox.setAlignment(Pos.CENTER_LEFT);
        hBox.getChildren().add(label);
        leftPane.getChildren().add(hBox);
    }

    private Node createBottomPane() {
        var hBox = new HBox();
        var nextEvent = new Button();
        nextEvent.setGraphic(new ImageView("/play.png"));
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
            if (producer != null) {
                producer.cancel();
                producer = null;
                runGenerator.setDisable(false);
                pauseGenerator.setDisable(true);
            }
        });
        pauseGenerator.setDisable(true);
        hBox.getChildren().add(pauseGenerator);

        runGenerator.setGraphic(new ImageView("/right.png"));
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
                graphPane.addEventNode(guiEvent);
            }
            System.out.println(
                    graphPane.getMaxEventHeight()
            );
        });
    }


}
