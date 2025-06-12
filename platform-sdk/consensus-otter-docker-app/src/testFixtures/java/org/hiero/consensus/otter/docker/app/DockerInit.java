// SPDX-License-Identifier: Apache-2.0
package org.hiero.consensus.otter.docker.app;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DockerInit {

    private final static Logger LOGGER = LogManager.getLogger(DockerInit.class);




    public static void main(String[] args) throws Exception {

        final var app = DockerApp.create();
        app.platform().start();

        while (true) {
            Thread.sleep(2000);
            LOGGER.info("Alive");
        }

    }

}
