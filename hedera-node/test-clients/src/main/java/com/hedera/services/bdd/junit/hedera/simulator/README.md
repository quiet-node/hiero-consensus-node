# Block Node Simulator

This package contains classes for simulating block nodes in tests. The simulator provides a way to test how consensus nodes handle different response codes from block nodes.

## Overview

The block node simulator consists of the following components:

- `SimulatedBlockNodeServer`: A gRPC server that implements the block streaming service. It can be configured to respond with different response codes.
- `BlockNodeSimulatorController`: A utility class to control simulated block node servers in a `SubProcessNetwork`.

## Usage

### Setting up a network with simulated block nodes

```java
// Create a network with simulated block nodes
SubProcessNetwork network = (SubProcessNetwork) SubProcessNetwork.newSharedNetwork(3);
network.setBlockNodeMode(BlockNodeMode.SIMULATOR);
network.start();

// Get the controller for the simulated block nodes
BlockNodeSimulatorController controller = network.getBlockNodeSimulatorController();
```

### Configuring response codes

You can configure the simulated block nodes to respond with specific response codes on the next block item:

```java
// Configure all simulators to respond with a specific EndOfStream response code
controller.setEndOfStreamResponse(PublishStreamResponseCode.STREAM_ITEMS_TIMEOUT, 123456L);

// Configure a specific simulator to respond with a specific EndOfStream response code
controller.setEndOfStreamResponse(0, PublishStreamResponseCode.STREAM_ITEMS_BAD_STATE_PROOF, 123456L);
```

### Sending immediate EndOfStream responses

You can also send EndOfStream responses immediately to all active streams, without waiting for the next block item:

```java
// Send an immediate EndOfStream response to all simulators
controller.sendEndOfStreamImmediately(PublishStreamResponseCode.STREAM_ITEMS_INTERNAL_ERROR, 123456L);

// Send an immediate EndOfStream response to a specific simulator
controller.sendEndOfStreamImmediately(0, PublishStreamResponseCode.STREAM_ITEMS_BEHIND, 123456L);
```

### Simulating connection drops

You can simulate connection drops by shutting down simulators and then restarting them:

```java
// Shutdown a specific simulator to simulate connection drop
controller.shutdownSimulator(0);

// Shutdown all simulators
controller.shutdownAllSimulators();

// Restart a specific simulator
controller.restartSimulator(0);

// Restart all simulators
controller.restartAllSimulators();
```

### Resetting responses

You can reset all configured responses to default behavior:

```java
// Reset all responses on all simulators
controller.resetAllResponses();

// Reset all responses on a specific simulator
controller.resetResponses(0);
```

## Available Response Codes

The following response codes are available from `PublishStreamResponseCode`:

- `STREAM_ITEMS_UNKNOWN`: An "unset value" flag, this value SHALL NOT be used.
- `STREAM_ITEMS_SUCCESS`: The request succeeded. No errors occurred and the source node orderly ended the stream.
- `STREAM_ITEMS_TIMEOUT`: The delay between items was too long. The source MUST start a new stream before the failed block.
- `STREAM_ITEMS_OUT_OF_ORDER`: An item was received out-of-order. The source MUST start a new stream before the failed block.
- `STREAM_ITEMS_BAD_STATE_PROOF`: A block state proof item could not be validated. The source MUST start a new stream before the failed block.
- `STREAM_ITEMS_BEHIND`: The block node is "behind" the publisher. The publisher may retry by sending blocks immediately following the `block_number` returned, or may end the stream and try again later.
- `STREAM_ITEMS_INTERNAL_ERROR`: The block node had an internal error and cannot continue processing. The publisher may retry again later.
- `STREAM_ITEMS_NOT_AVAILABLE`: The requested stream is not available. The publisher may retry again later.

## Example Test

See `BlockNodeSimulatorTest` for a complete example of how to use the block node simulator in tests. 