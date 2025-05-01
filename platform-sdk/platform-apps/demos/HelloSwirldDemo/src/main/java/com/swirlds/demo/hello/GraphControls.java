package com.swirlds.demo.hello;

import java.util.Timer;
import java.util.TimerTask;
import javafx.application.Platform;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollBar;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.paint.PhongMaterial;
import javafx.scene.shape.Rectangle;
import javafx.scene.shape.Sphere;

public class GraphControls {

    private GuiGraphGenerator graphGenerator;
    private GraphPane graphPane;
    private Timer timer;
    private TimerTask producer;
    private ScrollBar scrollBar;
    private Label selectedLabel;

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

        scrollBar.setMin(999);
        scrollBar.setMax(1000);
        scrollBar.setValue(999);

        scrollBar.valueProperty().addListener((observable, oldValue, newValue) -> {
            double val = scrollBar.getMax()- newValue.intValue();
            graphPane.setGroupYTranslation((int)val);
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
        leftPane.getChildren().add(createPlayerPane());
        this.selectedLabel = new Label("Firstline\nSecond line");
        leftPane.getChildren().add(selectedLabel);
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

    private Node createPlayerPane() {
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
            double distance = scrollBar.getMax()-scrollBar.getValue();
            scrollBar.setMax(graphPane.getTotalHeight());
            if ( scrollBar.getValue() != scrollBar.getMin() )  {
                //graphPane.setGroupYTranslation((int) (scrollBar.getMax()- distance));
                scrollBar.setValue(scrollBar.getMax()- distance);
            } else {
                graphPane.setGroupYTranslation(graphPane.getTotalHeight()-999);
            }
        });
    }


}
