// SPDX-License-Identifier: Apache-2.0
package com.swirlds.demo.hello;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.util.List;

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
        graphPane.setup();

        graphPane.addEventNode(new GuiEvent(Bytes.wrap("1"), 1, 1, 1, List.of(), 1, true, true, true, List.of()), 100, 0);
        graphPane.addEventNode(new GuiEvent(Bytes.wrap("2"), 2, 2, 2, List.of(), 1, true, true, true, List.of()), 200, 0);
        graphPane.addEventNode(new GuiEvent(Bytes.wrap("3"), 3, 3, 3, List.of(), 1, true, true, true, List.of()), 300, 0);

        VBox vBox = new VBox();
        vBox.getChildren().add(graphPane);
        vBox.getChildren().add(btn);
        primaryStage.setScene(new Scene(vBox, 1500, 1000));
        primaryStage.show();
    }
}

