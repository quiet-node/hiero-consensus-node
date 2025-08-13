// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.suites.misc;

import static com.hedera.services.bdd.junit.TestTags.ISS;
import static com.hedera.services.bdd.junit.hedera.ExternalPath.APPLICATION_PROPERTIES;
import static com.hedera.services.bdd.junit.hedera.NodeSelector.byNodeId;
import static com.hedera.services.bdd.junit.hedera.utils.WorkingDirUtils.updateBootstrapProperties;
import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.queries.QueryVerbs.getVersionInfo;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.cryptoTransfer;
import static com.hedera.services.bdd.spec.transactions.token.TokenMovement.movingHbar;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.assertHgcaaLogContains;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.assertHgcaaLogDoesNotContain;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.doingContextual;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.freezeOnly;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.sleepForSeconds;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.sourcing;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.waitForFrozenNetwork;
import static com.hedera.services.bdd.suites.HapiSuite.GENESIS;
import static com.hedera.services.bdd.suites.crypto.ParseableIssBlockStreamValidationOp.ISS_NODE_ID;
import static com.hedera.services.bdd.suites.regression.system.LifecycleTest.configVersionOf;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.services.bdd.junit.HapiTest;
import com.hedera.services.bdd.junit.hedera.NodeSelector;
import com.hedera.services.bdd.suites.crypto.ParseableIssBlockStreamValidationOp;
import com.hedera.services.bdd.suites.regression.system.LifecycleTest;
import com.hederahashgraph.api.proto.java.SemanticVersion;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Validates ISS detection works by reconnecting {@code node1} with an artificially low override for
 * {@code ledger.transfers.maxLen}, then submitting a {@link com.hedera.hapi.node.token.CryptoTransferTransactionBody}
 * that exceeds that artificial limit.
 * <p>
 * This should cause an ISS to be detected in {@code node1}, and the block stream manager to complete its fatal shutdown
 * process. The remaining nodes should still be able to handle transactions and freeze the network.
 */
@Tag(ISS)
class IssHandlingTest implements LifecycleTest {
    private static final Logger log = LogManager.getLogger(IssHandlingTest.class);

    @HapiTest
    final Stream<DynamicTest> simulateIss() {
        final AtomicReference<SemanticVersion> startVersion = new AtomicReference<>();
        final AtomicReference<List<String>> matchingLines = new AtomicReference<>(List.of());
        return hapiTest(
                getVersionInfo().exposingServicesVersionTo(startVersion::set),
                // Wait long enough for node1 to have typically written round 1 snapshot
                // to disk; restarting from this boundary snpshot can surface edge cases
                sleepForSeconds(2),
                // Reconnect node1 with an aberrant ledger.transfers.maxLen override
                sourcing(() -> reconnectIssNode(
                        byNodeId(ISS_NODE_ID),
                        configVersionOf(startVersion.get()),
                        // Before restarting node0, update its application properties to have a low transfer limit
                        doingContextual(spec -> {
                            final var loc = spec.getNetworkNodes()
                                    .get((int) ISS_NODE_ID)
                                    .getExternalPath(APPLICATION_PROPERTIES);
                            log.info("Setting artificial transfer limit @ {}", loc);
                            updateBootstrapProperties(loc, Map.of("ledger.transfers.maxLen", "5"));
                        }))),
                assertHgcaaLogContains(
                        NodeSelector.byNodeId(ISS_NODE_ID), "ledger.transfers.maxLen = 5", Duration.ofSeconds(10)),
                // First assert there was no ISS caused by simply reconnecting
                assertHgcaaLogDoesNotContain(
                        NodeSelector.byNodeId(ISS_NODE_ID), "ISS detected", Duration.ofSeconds(30)),

                // But now submit a transaction within the normal allowed transfers.maxLen limit, while
                // _not_ within the artificial limit set on the reconnected node
                cryptoTransfer(movingHbar(6L).distributing(GENESIS, "3", "4", "5", "6", "7", "8"))
                        .signedBy(GENESIS)
                        .via("issTransfer"),
                // Verify we actually got an ISS in node1
                assertHgcaaLogContains(NodeSelector.byNodeId(ISS_NODE_ID), "ISS detected", Duration.ofSeconds(60)),
                // Verify the block stream manager completed its fatal shutdown process
                assertHgcaaLogContains(
                        NodeSelector.byNodeId(ISS_NODE_ID),
                        "Block stream fatal shutdown complete",
                        Duration.ofSeconds(30)),
                // Verify the ISS Record Stream and Block Stream block files were written to S3 bucket
                assertHgcaaLogContains(
                                NodeSelector.byNodeId(ISS_NODE_ID),
                                "Successfully wrote Block Stream file",
                                Duration.ofSeconds(30))
                        .exposingLines(matchingLines),
                assertHgcaaLogContains(
                        NodeSelector.byNodeId(ISS_NODE_ID),
                        "Successfully wrote Record Stream file",
                        Duration.ofSeconds(30)),
                doingContextual(spec -> verifyIssTransactionBytesInBlockStreamBlock(
                        matchingLines, spec.registry().getBytes("issTransfer"))),
                // Submit a freeze
                freezeOnly().startingIn(2).seconds(),
                waitForFrozenNetwork(FREEZE_TIMEOUT, NodeSelector.exceptNodeIds(ISS_NODE_ID)),
                // And do some more validations
                new ParseableIssBlockStreamValidationOp());
    }

    private void verifyIssTransactionBytesInBlockStreamBlock(
            AtomicReference<List<String>> matchingLines, byte[] issCryptoTransferBytes) {
        if (!matchingLines.get().isEmpty()) {
            try {
                // Extract the path from the log line
                final String blockPath =
                        matchingLines.get().getFirst().split("path: ")[1].trim();
                log.info("Verifying ISS Block file exists at path: {}", blockPath);

                // Read file bytes into memory
                byte[] fileBytes = Files.readAllBytes(Path.of(blockPath));

                // First Uncompress the gzipped content
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fileBytes);
                GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
                byte[] decompressedData = gzipInputStream.readAllBytes();
                gzipInputStream.close();
                byteArrayInputStream.close();
                // Then parse the Block from the decompressed data
                Block storedBlock = Block.PROTOBUF.parse(Bytes.wrap(decompressedData));
                Transaction issTransaction = Transaction.PROTOBUF.parse(Bytes.wrap(issCryptoTransferBytes));

                log.info("Verifying ISS SignedTransaction Bytes are present in the Block Stream ISS Block file...");

                // Verify the ISS Transaction Bytes are present in the ISS Block file
                for (BlockItem item : storedBlock.items()) {
                    if (item.hasSignedTransaction()) {
                        Bytes signedTransaction = item.signedTransaction();
                        if (Arrays.equals(
                                signedTransaction.toByteArray(),
                                issTransaction.signedTransactionBytes().toByteArray())) {
                            log.info("ISS SignedTransaction Bytes found in ISS Block file at path: {}", blockPath);
                            return; // Found the transaction, exit the loop
                        }
                    }
                }
                log.error("ISS SignedTransaction Bytes not found in ISS Block file at path: {}", blockPath);
                throw new RuntimeException(
                        "ISS SignedTransaction Bytes not found in ISS Block Stream Block file in S3 bucket");
            } catch (Exception e) {
                log.error("Error verifying ISS Block file in S3 bucket: {}", e.getMessage());
                throw new RuntimeException(
                        "Failed to verify ISS Transaction Bytes present in ISS Block files in S3 bucket", e);
            }
        }
    }
}
