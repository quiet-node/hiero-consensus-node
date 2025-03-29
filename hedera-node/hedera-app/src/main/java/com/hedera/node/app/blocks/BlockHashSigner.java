// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletableFuture;

/**
 * Provides the ability to asynchronously sign a block hash.
 */
public interface BlockHashSigner {
    /**
     * The active and next signing scheme ids for this signer at a point in the block stream
     * where a proof is needed. An id of zero indicates that the scheme is not available.
     * @param activeId the active scheme id, if available
     * @param nextId the next scheme id, if available
     */
    record SchemeIds(long activeId, long nextId) {
        /**
         * Creates a new scheme ids object with just an active id.
         * @param id the active id
         * @return the scheme ids
         */
        public static SchemeIds fromNewlyAdopted(final long id) {
            return new SchemeIds(id, 0);
        }

        /**
         * Creates a new scheme ids object from this one, substituting the next id.
         * @param id the next id
         * @return the scheme ids
         */
        public SchemeIds withNextId(final long id) {
            return new SchemeIds(activeId, id);
        }
    }

    /**
     * Whether the signer is ready.
     */
    boolean isReady();

    /**
     * Returns a future that resolves to the signature of the given block hash.
     *
     * @param blockHash the block hash
     * @return the future
     */
    CompletableFuture<Bytes> signFuture(@NonNull Bytes blockHash);

    /**
     * Returns the scheme ids this signer is currently using at a point in the block stream
     * where a proof is needed.
     */
    SchemeIds currentSchemeIds();

    /**
     * Returns the verification key for the active signing scheme.
     */
    Bytes activeVerificationKey();
}
