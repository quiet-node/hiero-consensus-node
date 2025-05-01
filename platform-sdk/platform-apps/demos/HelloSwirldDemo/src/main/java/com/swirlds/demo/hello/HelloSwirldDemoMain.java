// SPDX-License-Identifier: Apache-2.0
package com.swirlds.demo.hello;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import javafx.application.Application;
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

        primaryStage.setTitle("Graph Visualizer");
        GraphControls controls = new GraphControls();
        primaryStage.setScene(new Scene(controls.setup(graphGenerator), 1500, 1000));
        primaryStage.show();
    }
}

