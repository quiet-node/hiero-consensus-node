// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.junit.hedera.simulator;

import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
import com.hedera.services.bdd.junit.hedera.subprocess.SubProcessNetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class to control simulated block node servers in a SubProcessNetwork.
 * This allows tests to induce specific response codes for testing error handling and edge cases.
 */
public class BlockNodeSimulatorController {
    private static final Logger log = LogManager.getLogger(BlockNodeSimulatorController.class);

    private final List<SimulatedBlockNodeServer> simulatedBlockNodes;
    // Store the ports of shutdown simulators for restart
    private final Map<Integer, Integer> shutdownSimulatorPorts = new HashMap<>();

    /**
     * Create a controller for the given network's simulated block nodes.
     *
     * @param network the SubProcessNetwork containing simulated block nodes
     */
    public BlockNodeSimulatorController(SubProcessNetwork network) {
        this.simulatedBlockNodes = network.getSimulatedBlockNodes();
        if (simulatedBlockNodes.isEmpty()) {
            log.warn("No simulated block nodes found in the network. Make sure BlockNodeMode.SIMULATOR is set.");
        } else {
            log.info("Controlling {} simulated block nodes", simulatedBlockNodes.size());
        }
    }

    /**
     * Configure all simulated block nodes to respond with a specific EndOfStream response code.
     *
     * @param responseCode the response code to send
     * @param blockNumber the block number to include in the response
     */
    public void setEndOfStreamResponse(PublishStreamResponseCode responseCode, long blockNumber) {
        for (SimulatedBlockNodeServer server : simulatedBlockNodes) {
            server.setEndOfStreamResponse(responseCode, blockNumber);
        }
        log.info("Set EndOfStream response code {} for block {} on all simulators", responseCode, blockNumber);
    }

    /**
     * Configure a specific simulated block node to respond with a specific EndOfStream response code.
     *
     * @param index the index of the simulated block node (0-based)
     * @param responseCode the response code to send
     * @param blockNumber the block number to include in the response
     */
    public void setEndOfStreamResponse(int index, PublishStreamResponseCode responseCode, long blockNumber) {
        if (index >= 0 && index < simulatedBlockNodes.size()) {
            simulatedBlockNodes.get(index).setEndOfStreamResponse(responseCode, blockNumber);
            log.info("Set EndOfStream response code {} for block {} on simulator {}",
                    responseCode, blockNumber, index);
        } else {
            log.error("Invalid simulator index: {}, valid range is 0-{}",
                    index, simulatedBlockNodes.size() - 1);
        }
    }

    /**
     * Send an EndOfStream response immediately to all active streams on all simulated block nodes.
     * This will end all active streams with the specified response code.
     *
     * @param responseCode the response code to send
     * @param blockNumber the block number to include in the response
     */
    public void sendEndOfStreamImmediately(PublishStreamResponseCode responseCode, long blockNumber) {
        for (SimulatedBlockNodeServer server : simulatedBlockNodes) {
            server.sendEndOfStreamImmediately(responseCode, blockNumber);
        }
        log.info("Sent immediate EndOfStream response with code {} for block {} on all simulators",
                responseCode, blockNumber);
    }

    /**
     * Send an EndOfStream response immediately to all active streams on a specific simulated block node.
     * This will end all active streams with the specified response code.
     *
     * @param index the index of the simulated block node (0-based)
     * @param responseCode the response code to send
     * @param blockNumber the block number to include in the response
     */
    public void sendEndOfStreamImmediately(int index, PublishStreamResponseCode responseCode, long blockNumber) {
        if (index >= 0 && index < simulatedBlockNodes.size()) {
            simulatedBlockNodes.get(index).sendEndOfStreamImmediately(responseCode, blockNumber);
            log.info("Sent immediate EndOfStream response with code {} for block {} on simulator {}",
                    responseCode, blockNumber, index);
        } else {
            log.error("Invalid simulator index: {}, valid range is 0-{}",
                    index, simulatedBlockNodes.size() - 1);
        }
    }

    /**
     * Reset all configured responses on all simulated block nodes to default behavior.
     */
    public void resetAllResponses() {
        for (SimulatedBlockNodeServer server : simulatedBlockNodes) {
            server.resetResponses();
        }
        log.info("Reset all responses on all simulators to default behavior");
    }

    /**
     * Reset all configured responses on a specific simulated block node to default behavior.
     *
     * @param index the index of the simulated block node (0-based)
     */
    public void resetResponses(int index) {
        if (index >= 0 && index < simulatedBlockNodes.size()) {
            simulatedBlockNodes.get(index).resetResponses();
            log.info("Reset all responses on simulator {} to default behavior", index);
        } else {
            log.error("Invalid simulator index: {}, valid range is 0-{}",
                    index, simulatedBlockNodes.size() - 1);
        }
    }

    /**
     * Get the number of simulated block nodes being controlled.
     *
     * @return the number of simulated block nodes
     */
    public int getSimulatorCount() {
        return simulatedBlockNodes.size();
    }

    /**
     * Shutdown all simulated block nodes to simulate connection drops.
     * The servers can be restarted using {@link #restartAllSimulators()}.
     */
    public void shutdownAllSimulators() {
        shutdownSimulatorPorts.clear();
        for (int i = 0; i < simulatedBlockNodes.size(); i++) {
            SimulatedBlockNodeServer server = simulatedBlockNodes.get(i);
            int port = server.getPort();
            shutdownSimulatorPorts.put(i, port);
            server.stop();
        }
        log.info("Shutdown all {} simulators to simulate connection drops", simulatedBlockNodes.size());
    }

    /**
     * Shutdown a specific simulated block node to simulate a connection drop.
     * The server can be restarted using {@link #restartSimulator(int)}.
     *
     * @param index the index of the simulated block node (0-based)
     */
    public void shutdownSimulator(int index) {
        if (index >= 0 && index < simulatedBlockNodes.size()) {
            SimulatedBlockNodeServer server = simulatedBlockNodes.get(index);
            int port = server.getPort();
            shutdownSimulatorPorts.put(index, port);
            server.stop();
            log.info("Shutdown simulator {} on port {} to simulate connection drop", index, port);
        } else {
            log.error("Invalid simulator index: {}, valid range is 0-{}",
                    index, simulatedBlockNodes.size() - 1);
        }
    }

    /**
     * Restart all previously shutdown simulated block nodes.
     * This will recreate the servers on the same ports they were running on before shutdown.
     *
     * @throws IOException if a server fails to start
     */
    public void restartAllSimulators() throws IOException {
        List<Integer> successfullyRestarted = new ArrayList<>();

        for (Map.Entry<Integer, Integer> entry : shutdownSimulatorPorts.entrySet()) {
            int index = entry.getKey();
            int port = entry.getValue();

            if (index >= 0 && index < simulatedBlockNodes.size()) {
                // Create a new server on the same port
                SimulatedBlockNodeServer newServer = new SimulatedBlockNodeServer(port);
                newServer.start();

                // Replace the old server in the list
                simulatedBlockNodes.set(index, newServer);
                successfullyRestarted.add(index);

                log.info("Restarted simulator {} on port {}", index, port);
            }
        }

        // Clear the ports of successfully restarted simulators
        for (Integer index : successfullyRestarted) {
            shutdownSimulatorPorts.remove(index);
        }

        log.info("Restarted {} simulators", successfullyRestarted.size());
    }

    /**
     * Restart a specific previously shutdown simulated block node.
     * This will recreate the server on the same port it was running on before shutdown.
     *
     * @param index the index of the simulated block node (0-based)
     * @throws IOException if the server fails to start
     */
    public void restartSimulator(int index) throws IOException {
        if (!shutdownSimulatorPorts.containsKey(index)) {
            log.error("Simulator {} was not previously shutdown or has already been restarted", index);
            return;
        }

        if (index >= 0 && index < simulatedBlockNodes.size()) {
            int port = shutdownSimulatorPorts.get(index);

            // Create a new server on the same port
            SimulatedBlockNodeServer newServer = new SimulatedBlockNodeServer(port);
            newServer.start();

            // Replace the old server in the list
            simulatedBlockNodes.set(index, newServer);

            // Remove from the shutdown map
            shutdownSimulatorPorts.remove(index);

            log.info("Restarted simulator {} on port {}", index, port);
        } else {
            log.error("Invalid simulator index: {}, valid range is 0-{}",
                    index, simulatedBlockNodes.size() - 1);
        }
    }
} 