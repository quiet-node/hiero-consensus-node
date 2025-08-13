// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.cache;

import static com.hedera.node.app.blocks.impl.streaming.FileBlockItemWriter.COMPLETE_BLOCK_EXTENSION;
import static com.hedera.node.app.blocks.impl.streaming.FileBlockItemWriter.COMPRESSION_ALGORITHM_EXTENSION;
import static com.hedera.node.app.blocks.impl.streaming.FileBlockItemWriter.longToFileName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.node.app.records.impl.producers.formats.BlockRecordWriterFactoryImpl;
import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.VersionedConfigImpl;
import com.hedera.node.config.data.BlockStreamConfig;
import com.hedera.node.config.data.IssContextConfig;
import com.hedera.node.config.testfixtures.HederaTestConfigBuilder;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.platform.test.fixtures.simulated.RandomSigner;
import com.swirlds.state.lifecycle.info.NetworkInfo;
import com.swirlds.state.lifecycle.info.NodeInfo;
import java.io.ByteArrayInputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExecutionOutputCacheTest {

    @Mock
    private ConfigProvider configProvider;

    @Mock
    private NetworkInfo networkInfo;

    @TempDir
    Path tempDir;

    @Test
    void testUploadBlockStreamBlock() throws Exception {
        NodeInfo mockSelfNodeInfo = mock(NodeInfo.class);

        final var config = HederaTestConfigBuilder.create()
                .withConfigDataType(BlockStreamConfig.class)
                .withValue("blockStream.blockPeriod", Duration.of(2, ChronoUnit.SECONDS))
                .withConfigDataType(IssContextConfig.class)
                .withValue("issContextConfig.enabled", "true")
                .withValue("issContextConfig.diskPath", tempDir.toAbsolutePath())
                .withValue("issContextConfig.recordBlockBufferSize", "5")
                .getOrCreateConfig();
        given(configProvider.getConfiguration()).willReturn(new VersionedConfigImpl(config, 1L));

        ExecutionOutputCache executionOutputCache = new ExecutionOutputCache(
                configProvider,
                networkInfo,
                new BlockRecordWriterFactoryImpl(
                        configProvider, mockSelfNodeInfo, new RandomSigner(new Random()), mock(FileSystem.class)));

        BlockItem blockItem = BlockItem.newBuilder()
                .blockHeader(BlockHeader.newBuilder().number(42).build())
                .build();

        executionOutputCache.createBlock(42);
        executionOutputCache.addBlockItem(42, blockItem);
        executionOutputCache.addBlockItem(
                42,
                BlockItem.newBuilder()
                        .roundHeader(RoundHeader.newBuilder().roundNumber(53).build())
                        .build());
        executionOutputCache.setIssRoundNumber(53);
        executionOutputCache.handleIssContextualBlocks();

        // Read file bytes into memory
        byte[] fileBytes = Files.readAllBytes(tempDir.resolve("53"
                + "/blockStream/" + longToFileName(42) + COMPLETE_BLOCK_EXTENSION
                + COMPRESSION_ALGORITHM_EXTENSION));

        // First Uncompress the gzipped content
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fileBytes);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
        byte[] decompressedData = gzipInputStream.readAllBytes();
        gzipInputStream.close();
        byteArrayInputStream.close();
        // Then parse the Block from the decompressed data
        Block storedBlock = Block.PROTOBUF.parse(Bytes.wrap(decompressedData));
        // Assert the block matches
        assertEquals(storedBlock.items().getFirst().blockHeader().number(), 42L);
    }
}
