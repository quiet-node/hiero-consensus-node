// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.contract.impl.test.exec.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;

import com.hedera.node.app.service.contract.impl.exec.metrics.SystemContractOpsDurationMetric;
import com.hedera.node.config.testfixtures.HederaTestConfigBuilder;
import com.swirlds.common.metrics.config.MetricsConfig;
import com.swirlds.common.metrics.platform.DefaultPlatformMetrics;
import com.swirlds.common.metrics.platform.MetricKeyRegistry;
import com.swirlds.common.metrics.platform.PlatformMetricsFactoryImpl;
import java.util.concurrent.Executors;
import org.hiero.consensus.model.node.NodeId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SystemContractOpsDurationMetricTest {
    private SystemContractOpsDurationMetric subject;

    private final String systemContract1 = "sc1";
    private final String address1 = "cafebabe";

    private final String systemContract2 = "sc2";
    private final String address2 = "babecafe";

    @BeforeEach
    void setUp() {
        final MetricsConfig metricsConfig =
                HederaTestConfigBuilder.createConfig().getConfigData(MetricsConfig.class);

        final var metrics = new DefaultPlatformMetrics(
                NodeId.of(3),
                new MetricKeyRegistry(),
                Executors.newSingleThreadScheduledExecutor(),
                new PlatformMetricsFactoryImpl(metricsConfig),
                metricsConfig);

        subject = new SystemContractOpsDurationMetric(metrics);
    }

    @Test
    void recordsAndRetrievesOperationDuration() {
        // Given
        final long duration1 = 100L;
        final long duration2 = 200L;

        // When
        subject.recordOperationDuration(systemContract1, address1, duration1);
        subject.recordOperationDuration(systemContract1, address1, duration2);

        // Then
        final double average = subject.getSystemContractOpsDuration(systemContract1, address1)
                .average()
                .get();

        final double count = subject.getSystemContractOpsDuration(systemContract1, address1)
                .counter()
                .get();
        final double total = subject.getSystemContractOpsDuration(systemContract1, address1)
                .accumulator()
                .get();
        assertThat(average).isCloseTo(150.0, within(5.0)); // (100 + 200) / 2
        assertThat(count).isEqualTo(2.0); // Two durations recorded
        assertThat(total).isEqualTo(300.0); // 100 + 200
    }

    @Test
    void returnsZeroForNonExistentMethod() {
        final var metric = subject.getSystemContractOpsDuration(systemContract2, address2);
        assertThat(metric.average().get()).isZero();
        assertThat(metric.accumulator().get()).isZero();
        assertThat(metric.counter().get()).isZero();
    }

    @Test
    void handlesMultipleMethods() {
        // Given
        final long duration1 = 100L;
        final long duration2 = 200L;
        final long duration3 = 300L;

        // When
        subject.recordOperationDuration(systemContract1, address1, duration1);
        subject.recordOperationDuration(systemContract1, address1, duration3);
        subject.recordOperationDuration(systemContract1, address1, duration1);
        subject.recordOperationDuration(systemContract1, address1, duration3);
        subject.recordOperationDuration(systemContract2, address2, duration2);
        subject.recordOperationDuration(systemContract2, address2, duration2);

        // Then
        final var metric1 = subject.getSystemContractOpsDuration(systemContract1, address1);
        final var metric2 = subject.getSystemContractOpsDuration(systemContract2, address2);
        assertThat(metric1.average().get()).isCloseTo(200.0, within(5.0));
        assertThat(metric1.counter().get()).isEqualTo(4);
        assertThat(metric1.accumulator().get()).isEqualTo(800L); // 100 + 300 + 100 + 300
        assertThat(metric2.average().get()).isCloseTo(200.0, within(5.0));
        assertThat(metric2.counter().get()).isEqualTo(2);
        assertThat(metric2.accumulator().get()).isEqualTo(400L); // 200 + 200
    }
}
