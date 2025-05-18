// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static com.hedera.node.app.blocks.impl.streaming.BlockNodeConnection.ConnectionState.ACTIVE;
import static com.hedera.node.app.blocks.impl.streaming.BlockNodeConnection.ConnectionState.UNINITIALIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponse.Acknowledgement;
import com.hedera.hapi.block.PublishStreamResponse.BlockAcknowledgement;
import com.hedera.node.app.metrics.BlockStreamMetrics;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.VersionedConfigImpl;
import com.hedera.node.config.testfixtures.HederaTestConfigBuilder;
import com.hedera.node.internal.network.BlockNodeConfig;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.lifecycle.info.NodeInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webclient.grpc.GrpcServiceClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link BlockNodeConnection}.
 */
@ExtendWith(MockitoExtension.class)
public class BlockNodeConnectionRedoneTest {

    // Block Node Communication Components
    private BlockNodeConnection subject;
    private BlockNodeConnectionManager blockNodeConnectionManager;
    private BlockStreamMetrics blockStreamMetrics;
    private BlockStreamStateManager blockStreamStateManager;
    private ConfigProvider configProvider;
    private BlockNodeConfig nodeConfig;

    @Mock
    private Metrics mockMetrics;

    @Mock
    private NodeInfo mockNodeInfo;

    @Mock
    private GrpcServiceClient mockGrpcServiceClient;

    @BeforeEach
    void setUp() {
        // Setup ConfigProvider
        configProvider = createConfigProvider();
        nodeConfig = new BlockNodeConfig("localhost", 8080, 1);

        // Setup BlockStreamMetrics with mocks
        when(mockNodeInfo.nodeId()).thenReturn(0L);
        blockStreamMetrics = new BlockStreamMetrics(mockMetrics, mockNodeInfo);

        // Setup BlockStreamStateManager
        blockStreamStateManager = new BlockStreamStateManager(configProvider, blockStreamMetrics);

        // Setup BlockNodeConnectionManager
        blockNodeConnectionManager = Mockito.spy(
                new BlockNodeConnectionManager(configProvider, blockStreamStateManager, blockStreamMetrics));

        blockStreamStateManager.setBlockNodeConnectionManager(blockNodeConnectionManager);

        // Set up BlockNodeConnection
        subject = new BlockNodeConnection(
                configProvider,
                nodeConfig,
                blockNodeConnectionManager,
                blockStreamStateManager,
                mockGrpcServiceClient,
                blockStreamMetrics);

        doReturn(subject).when(blockNodeConnectionManager).createBlockNodeConnection(any(), any());
    }

    @Test
    void testUpdateConnectionState() {
        blockNodeConnectionManager.waitForConnection(Duration.ofSeconds(5));

        // Given
        final var expectedState = UNINITIALIZED;

        // When
        subject.updateConnectionState(expectedState);

        // Then
        assertThat(subject.getConnectionState()).isEqualTo(expectedState);
    }

    @Test
    void testHandleAcknowledgementLastVerifiedBlockUpdated() {
        blockNodeConnectionManager.waitForConnection(Duration.ofSeconds(5));

        assertThat(blockStreamStateManager.getConnections().containsValue(subject))
                .isTrue();
        assertEquals(subject.getConnectionState(), ACTIVE);

        // Open Block 0
        blockStreamStateManager.openBlock(0L);

        // Create Test BlockAcknowledgement
        PublishStreamResponse response = createBlockAckResponse(0L, false);

        subject.onNext(response);

        assertEquals(blockNodeConnectionManager.getLastVerifiedBlock(nodeConfig), 0L);

        // Create Test BlockAcknowledgement
        response = createBlockAckResponse(123L, false);

        subject.onNext(response);

        assertEquals(blockNodeConnectionManager.getLastVerifiedBlock(nodeConfig), 123L);
    }

    @Test
    void testHandleAcknowledgementCurrentBlockStreamingNegativeCase() {
        blockNodeConnectionManager.waitForConnection(Duration.ofSeconds(5));

        assertThat(blockStreamStateManager.getConnections().containsValue(subject))
                .isTrue();
        assertEquals(subject.getConnectionState(), ACTIVE);

        // Create Test BlockAcknowledgement
        PublishStreamResponse response = createBlockAckResponse(0L, true);

        subject.onNext(response);

        assertEquals(0L, blockNodeConnectionManager.getLastVerifiedBlock(nodeConfig));

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(0L, blockNodeConnectionManager.getStreamingBlockNumber().get());
    }

    @Test
    void testHandleAcknowledgementJumpWhenAckBetweenStreamingAndProducing() {
        blockNodeConnectionManager.waitForConnection(Duration.ofSeconds(5));

        assertThat(blockStreamStateManager.getConnections().containsValue(subject))
                .isTrue();
        assertEquals(subject.getConnectionState(), ACTIVE);

        // Create Test BlockAcknowledgement
        PublishStreamResponse response = createBlockAckResponse(5L, true);

        // Open Block 0
        blockStreamStateManager.openBlock(0L);

        // Open Block 0
        blockStreamStateManager.openBlock(10L);

        subject.onNext(response);

        assertEquals(5L, blockNodeConnectionManager.getLastVerifiedBlock(nodeConfig));

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(6L, blockNodeConnectionManager.getStreamingBlockNumber().get());
    }

    @Test
    void testHandleAcknowledgementUnknown() {
        blockNodeConnectionManager.waitForConnection(Duration.ofSeconds(5));

        assertThat(blockStreamStateManager.getConnections().containsValue(subject))
                .isTrue();
        assertEquals(subject.getConnectionState(), ACTIVE);

        // Open Block 0
        blockStreamStateManager.openBlock(0L);

        Acknowledgement acknowledgement = Acknowledgement.newBuilder().build();

        PublishStreamResponse response = PublishStreamResponse.newBuilder()
                .acknowledgement(acknowledgement)
                .build();

        subject.onNext(response);

        assertEquals(-1L, blockNodeConnectionManager.getLastVerifiedBlock(nodeConfig));
    }

    @NonNull
    private static PublishStreamResponse createBlockAckResponse(long blockNumber, boolean alreadyExists) {
        BlockAcknowledgement blockAck = BlockAcknowledgement.newBuilder()
                .blockNumber(blockNumber)
                .blockAlreadyExists(alreadyExists)
                .build();

        Acknowledgement acknowledgement =
                Acknowledgement.newBuilder().blockAck(blockAck).build();

        return PublishStreamResponse.newBuilder()
                .acknowledgement(acknowledgement)
                .build();
    }

    private ConfigProvider createConfigProvider() {
        final var configPath = Objects.requireNonNull(
                        BlockNodeConnectionManagerTest.class.getClassLoader().getResource("bootstrap/"))
                .getPath();
        assertThat(Files.exists(Path.of(configPath))).isTrue();

        final var config = HederaTestConfigBuilder.create()
                .withValue("blockStream.writerMode", "FILE_AND_GRPC")
                .withValue("blockNode.blockNodeConnectionFileDir", configPath)
                .getOrCreateConfig();
        return () -> new VersionedConfigImpl(config, 1L);
    }
}
