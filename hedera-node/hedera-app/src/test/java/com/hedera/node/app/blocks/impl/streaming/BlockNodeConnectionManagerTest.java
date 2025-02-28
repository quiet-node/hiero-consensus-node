// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;

import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.VersionedConfigImpl;
import com.hedera.node.config.VersionedConfiguration;
import com.hedera.node.config.testfixtures.HederaTestConfigBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
class BlockNodeConnectionManagerTest {

    BlockNodeConnectionManager blockNodeConnectionManager;

    @Mock
    ConfigProvider mockConfigProvider;

    @Mock
    VersionedConfiguration mockVersionedConfiguration;

    @BeforeEach
    public void setUp() {
        final var config = HederaTestConfigBuilder.create()
                .withValue("blockStream.writerMode", "FILE_AND_GRPC")
                .withValue("blockStream.blockNodeConnectionFileDir", "./src/test/resources/bootstrap")
                .getOrCreateConfig();
        given(mockConfigProvider.getConfiguration()).willReturn(new VersionedConfigImpl(config, 1));
        blockNodeConnectionManager = new BlockNodeConnectionManager(mockConfigProvider);
    }

    @Test
    void testNewBlockNodeConnectionManager() {
        final var expectedGrpcEndpoint =
                BlockStreamServiceGrpc.getPublishBlockStreamMethod().getBareMethodName();
        assertEquals(expectedGrpcEndpoint, blockNodeConnectionManager.getGrpcEndPoint());
    }
}
