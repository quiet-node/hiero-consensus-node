// SPDX-License-Identifier: Apache-2.0
package com.swirlds.demo.hello;

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;

public class HelloSwirldDemoMain extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        final GuiGraphGenerator graphGenerator = new GuiGraphGenerator(1, 4);

        primaryStage.setTitle("Hello World!");
        Button btn = new Button();
        btn.setText("Say 'Hello World'");
        btn.setOnAction(new EventHandler<ActionEvent>() {

            @Override
            public void handle(ActionEvent event) {
                System.out.println("Hello World!");
            }
        });

        GraphPane graphPane = new GraphPane();
        var canvas = graphPane.setup();
        VBox vBox = new VBox();
        vBox.getChildren().add(canvas);
        vBox.getChildren().add(btn);
        primaryStage.setScene(new Scene(vBox, 1500, 1000));
        primaryStage.show();
    }


}

