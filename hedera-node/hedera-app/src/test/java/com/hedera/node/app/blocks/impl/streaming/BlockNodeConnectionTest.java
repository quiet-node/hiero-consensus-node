// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.node.app.spi.fixtures.util.LogCaptor;
import com.hedera.node.app.spi.fixtures.util.LogCaptureExtension;
import com.hedera.node.app.spi.fixtures.util.LoggingSubject;
import com.hedera.node.app.spi.fixtures.util.LoggingTarget;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, LogCaptureExtension.class})
class BlockNodeConnectionTest {
    private static final Duration INITIAL_DELAY = Duration.ofMillis(10);
    private static final int MAX_ATTEMPTS = 5;

    @LoggingSubject
    private BlockNodeConnection blockNodeConnection;

    @LoggingTarget
    private LogCaptor logCaptor;

    @Mock
    private Supplier<Void> mockSupplier;

    @BeforeEach
    public void setUp() {
        blockNodeConnection = new BlockNodeConnection(null, null, null);
    }

    @Test
    void testRetry_SuccessOnFirstAttempt() throws Exception {
        blockNodeConnection.retry(mockSupplier, INITIAL_DELAY, MAX_ATTEMPTS);

        verify(mockSupplier, times(1)).get();
    }

    @Test
    void testRetry_SuccessOnRetry() throws Exception {
        when(mockSupplier.get())
                .thenThrow(new RuntimeException("First attempt failed"))
                .thenReturn(null);

        blockNodeConnection.retry(mockSupplier, INITIAL_DELAY, MAX_ATTEMPTS);

        verify(mockSupplier, times(2)).get();
    }

    @Test
    void testRetry_FailureAfterMaxAttempts() {
        when(mockSupplier.get()).thenThrow(new RuntimeException("Fail every time"));

        Exception exception = assertThrows(Exception.class, () -> {
            blockNodeConnection.retry(mockSupplier, INITIAL_DELAY, MAX_ATTEMPTS);
        });

        assertEquals("Max retry attempts reached", exception.getMessage());
        assertEquals("Fail every time", exception.getCause().getMessage());
        verify(mockSupplier, times(MAX_ATTEMPTS)).get();

        assertThat(logCaptor.infoLogs())
                .contains(
                        "BlockNodeConnection INITIALIZED",
                        "Failed to execute action, retrying in 10 ms",
                        "Failed to execute action, retrying in 20 ms",
                        "Failed to execute action, retrying in 40 ms",
                        "Failed to execute action, retrying in 80 ms");
    }
}
