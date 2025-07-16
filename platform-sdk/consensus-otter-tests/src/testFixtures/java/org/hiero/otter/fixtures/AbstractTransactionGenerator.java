// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.consensus.model.status.PlatformStatus;

/**
 * Abstract base class for transaction generators. Provides common functionality for generating and submitting
 * random transactions.
 *
 * @see TransactionGenerator
 */
public abstract class AbstractTransactionGenerator implements TransactionGenerator {
    private static final Logger log = LogManager.getLogger();

    /** Random number generator used for creating randomized transactions. */
    protected final Random random;

    /**
     * Constructs a new AbstractTransactionGenerator with the specified random number generator.
     *
     * @param random the random number generator to use for transaction generation;
     *               must not be null
     * @throws NullPointerException if random is null
     */
    protected AbstractTransactionGenerator(@NonNull final Random random) {
        this.random = requireNonNull(random);
    }

    /**
     * Submits randomly generated transactions to all active nodes in the provided list.
     *
     * <p>This method iterates through all nodes and submits a random transaction
     * to each node that has an {@link PlatformStatus#ACTIVE} status. If a transaction
     * submission fails for any reason, the exception is logged but does not prevent
     * submission to other nodes.
     *
     * @param <T> the type of nodes in the list, must extend {@link Node}
     * @param nodes the list of nodes to submit transactions to; must not be null
     * @throws NullPointerException if nodes is null
     */
    protected <T extends Node> void submitRandomTransactions(@NonNull final List<T> nodes) {
        requireNonNull(nodes, "nodes must not be null");
        for (final Node node : nodes) {
            if (node.platformStatus() == PlatformStatus.ACTIVE) {
                try {
                    node.submitTransaction(generateRandomTransaction());
                } catch (final Exception exception) {
                    log.info("Unable to submit transaction to node {}", node.selfId(), exception);
                }
            }
        }
    }

    /**
     * Generates a random transaction
     *
     * @return a byte array representing a randomly generated transaction
     */
    private byte[] generateRandomTransaction() {
        return TransactionFactory.createEmptyTransaction(random.nextInt()).toByteArray();
    }
}
