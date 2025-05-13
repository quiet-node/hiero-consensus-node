// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.hedera.hapi.block.PublishStreamRequest;
import com.hedera.node.app.spi.fixtures.util.LogCaptureExtension;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.VersionedConfiguration;
import com.hedera.node.config.data.BlockStreamConfig;
import com.hedera.node.internal.network.BlockNodeConfig;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlockStreamProcessorTest {

    @Mock
    private ConfigProvider configProvider;
    @Mock
    private BlockStreamStateManager stateManager;
    @Mock
    private BlockStreamConfig config;
    @Mock
    private VersionedConfiguration versionedConfiguration;

    @BeforeEach
    public void setup() {
        when(configProvider.getConfiguration()).thenReturn(versionedConfiguration);
        when(versionedConfiguration.getConfigData(BlockStreamConfig.class)).thenReturn(config);
    }

    @Test
    public void testConstructorDoesNotStartThreadIfStreamingDisabled() {
        when(config.streamToBlockNodes()).thenReturn(false);

        BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);
        assertNotNull(processor);
        assertEquals(-1, processor.getBlockNumber().get());
    }

    @Test
    public void testConstructorStartsThreadIfStreamingEnabled() throws InterruptedException {
        when(config.streamToBlockNodes()).thenReturn(true);

        try (MockedStatic<Thread> threadMockedStatic = mockStatic(Thread.class)) {
            Thread.Builder builder = mock(Thread.Builder.class);
            Thread fakeThread = mock(Thread.class);
            threadMockedStatic.when(Thread::ofPlatform).thenReturn(builder);
            when(builder.name(anyString())).thenReturn(builder);
            when(builder.start(any(Runnable.class))).thenReturn(fakeThread);

            BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);
            assertNotNull(processor);
            assertEquals(-1, processor.getBlockNumber().get());

            threadMockedStatic.verify(() -> Thread.ofPlatform(), times(1));
        }
    }

    @Test
    public void testJumpTargetIsSettableAndReadable() {
        when(config.streamToBlockNodes()).thenReturn(false);

        BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);
        AtomicLong jumpTarget = processor.getJumpTargetBlock();
        jumpTarget.set(42);

        assertEquals(42, processor.getJumpTargetBlock().get());
    }

    @Test
    public void testBlockNumberIsSettableAndReadable() {
        when(config.streamToBlockNodes()).thenReturn(false);

        BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);
        AtomicLong blockNumber = processor.getBlockNumber();
        blockNumber.set(5);

        assertEquals(5, processor.getBlockNumber().get());
    }

    @Test
    public void testRunProcessesRequestsAndAdvancesBlock() throws Exception {
        when(config.streamToBlockNodes()).thenReturn(false);

        BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);

        BlockNodeConnection connection = mock(BlockNodeConnection.class);
        when(stateManager.getActiveConnection()).thenReturn(connection);

        BlockNodeConfig nodeConfig = mock(BlockNodeConfig.class);
        when(connection.getNodeConfig()).thenReturn(nodeConfig);
        when(nodeConfig.address()).thenReturn("127.0.0.1");
        when(nodeConfig.port()).thenReturn(8080);

        PublishStreamRequest request1 = mock(PublishStreamRequest.class);
        PublishStreamRequest request2 = mock(PublishStreamRequest.class);

        BlockState blockState = mock(BlockState.class);
        when(blockState.requests()).thenReturn(List.of(request1, request2));
        when(blockState.isComplete()).thenReturn(true);
        when(stateManager.getBlockState(10)).thenReturn(blockState);

        when(stateManager.higherPriorityStarted(connection)).thenReturn(false);

        processor.getBlockNumber().set(10);

        Thread testThread = new Thread(() -> {
            try {
                processor.run();
            } catch (Exception e) {
                // ignore
            }
        });

        testThread.start();
        Thread.sleep(50); // Let it loop a few times
        testThread.interrupt(); // force kill after test

        verify(connection, atLeastOnce()).sendRequest(any(PublishStreamRequest.class));
    }

    @Test
    public void testRunHandlesJumpSignalCorrectly() {
        when(config.streamToBlockNodes()).thenReturn(false);

        BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);
        processor.getJumpTargetBlock().set(20);

        Thread testThread = new Thread(() -> {
            try {
                processor.run();
            } catch (Exception e) {
                // ignore
            }
        });

        testThread.start();
        try {
            Thread.sleep(50);
        } catch (InterruptedException ignored) {
        }

        assertEquals(20, processor.getBlockNumber().get());

        testThread.interrupt(); // force kill after test
    }

    @Test
    public void testRunHandlesInterruptedExceptionDuringSleep() {
        when(config.streamToBlockNodes()).thenReturn(false);

        BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);

        Thread thread = new Thread(() -> {
            try {
                processor.run();
            } catch (RuntimeException e) {
                assertTrue(e.getCause() instanceof InterruptedException);
            }
        });

        thread.start();
        thread.interrupt(); // cause sleep interruption
        try {
            Thread.sleep(30);
        } catch (InterruptedException ignored) {
        }
        thread.interrupt(); // force kill after test
    }

    @Test
    public void testRunCatchesGenericExceptions() {
        when(config.streamToBlockNodes()).thenReturn(false);

        BlockStreamProcessor processor = new BlockStreamProcessor(configProvider, stateManager);
        when(stateManager.getActiveConnection()).thenThrow(new RuntimeException("Simulated"));

        Thread thread = new Thread(() -> {
            processor.run(); // Will log error but not crash
        });

        thread.start();
        try {
            Thread.sleep(30);
        } catch (InterruptedException ignored) {
        }
        thread.interrupt(); // force kill after test
    }
}
