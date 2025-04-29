// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.test;

import com.swirlds.logging.legacy.LogMarker;
import com.swirlds.common.io.utility.FileUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.otter.fixtures.Network;
import org.hiero.otter.fixtures.OtterTest;
import org.hiero.otter.fixtures.TestEnvironment;
import org.hiero.otter.fixtures.TimeManager;
import org.hiero.otter.fixtures.Validator.LogFilter;
import org.hiero.otter.fixtures.Validator.Profile;
import org.hiero.otter.fixtures.turtle.TurtleTestEnvironment;
import org.junit.jupiter.api.Disabled;

public class HappyPathTest {

    @OtterTest
//    @Disabled
    void testHappyPath(TestEnvironment env) throws InterruptedException {
        int i = 0;
        while (true) {
            i++;
            System.out.println("Iteration: " + i);
            env.start();
            final Network network = env.network();
            final TimeManager timeManager = env.timeManager();

            // Setup simulation
            network.addNodes(4);
            network.start(Duration.ofMinutes(1L));

            // Wait for two minutes
            timeManager.waitFor(Duration.ofMinutes(2L));

        // Validations
//        env.validator()
//                .assertLogs(
//                        LogFilter.maxLogLevel(Level.INFO),
//                        LogFilter.ignoreMarkers(LogMarker.STARTUP),
//                        LogFilter.ignoreNodes(network.getNodes().getFirst()))
//                .validateRemaining(Profile.DEFAULT);
            // Validations
            env.validator().validateRemaining(Profile.DEFAULT);
//            env.destroy();
            env.stop();
            network.setState();
        }
    }
}
