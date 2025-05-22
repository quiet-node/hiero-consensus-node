// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.node.config.ConfigProvider;
import com.hedera.node.config.VersionedConfigImpl;
import com.hedera.node.config.testfixtures.HederaTestConfigBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.api.PublishStreamResponse.EndOfStream;
import org.hiero.block.api.PublishStreamResponse.ResendBlock;
import org.hiero.block.api.PublishStreamResponse.SkipBlock;

/**
 * Base class for tests that involve block node communication.
 */
public abstract class BlockNodeCommunicationTestBase {

    protected static final long BATCH_SIZE = 5L;

    @NonNull
    protected static PublishStreamResponse createSkipBlock(long blockNumber) {
        SkipBlock skipBlock = SkipBlock.newBuilder().blockNumber(blockNumber).build();

        return PublishStreamResponse.newBuilder().skipBlock(skipBlock).build();
    }

    @NonNull
    protected static PublishStreamResponse createResendBlock(long blockNumber) {
        ResendBlock resendBlock =
                ResendBlock.newBuilder().blockNumber(blockNumber).build();

        return PublishStreamResponse.newBuilder().resendBlock(resendBlock).build();
    }

    @NonNull
    protected static PublishStreamResponse createEndOfStreamResponse(
            EndOfStream.Code responseCode, long lastVerifiedBlock) {
        EndOfStream eos = EndOfStream.newBuilder()
                .blockNumber(lastVerifiedBlock)
                .status(responseCode)
                .build();

        return PublishStreamResponse.newBuilder().endStream(eos).build();
    }

    @NonNull
    protected static PublishStreamResponse createBlockAckResponse(long blockNumber, boolean alreadyExists) {
        BlockAcknowledgement acknowledgement = BlockAcknowledgement.newBuilder()
                .blockNumber(blockNumber)
                .blockAlreadyExists(alreadyExists)
                .build();

        return PublishStreamResponse.newBuilder()
                .acknowledgement(acknowledgement)
                .build();
    }

    protected ConfigProvider createConfigProvider() {
        final var configPath = Objects.requireNonNull(
                        BlockNodeConnectionManagerTest.class.getClassLoader().getResource("bootstrap/"))
                .getPath();
        assertThat(Files.exists(Path.of(configPath))).isTrue();

        final var config = HederaTestConfigBuilder.create()
                .withValue("blockStream.writerMode", "FILE_AND_GRPC")
                .withValue("blockNode.blockNodeConnectionFileDir", configPath)
                .withValue("blockStream.blockItemBatchSize", BATCH_SIZE)
                .getOrCreateConfig();
        return () -> new VersionedConfigImpl(config, 1L);
    }
}
