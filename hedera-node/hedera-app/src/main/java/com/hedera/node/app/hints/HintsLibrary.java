// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.hints;

import com.hedera.cryptography.hints.AggregationAndVerificationKeys;
import com.hedera.node.app.hints.impl.HintsLibraryCodec;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/**
 * The cryptographic operations required by the {@link HintsService}.
 * <p>
 * The relationship between the hinTS algorithms and these operations are as follows:
 * <ul>
 *   <li><b>CRS creation</b> ({@code Setup}) - Implemented by using {@link HintsLibrary#newCrs(int)},
 *   {@link HintsLibrary#updateCrs(byte[], byte[])}, and {@link HintsLibrary#verifyCrsUpdate(byte[], byte[], byte[])}.</li>
 *   <li><b>Key generation</b> ({@code KGen}) - Implemented by {@link HintsLibrary#newBlsKeyPair()}.</li>
 *   <li><b>Hint generation</b> ({@code HintGen}) - Implemented by {@link HintsLibrary#computeHints(byte[], byte[], int, int)}.</li>
 *   <li><b>Preprocessing</b> ({@code Preprocess}) - Implemented by using {@link HintsLibrary#preprocess(byte[], Map, Map, int)}
 *   to select the hinTS keys to use as input to {@link HintsLibrary#preprocess(byte[], Map, Map, int)}.</li>
 *   <li><b>Partial signatures</b> ({@code Sign}) - Implemented by {@link HintsLibrary#signBls(byte[], byte[])}.</li>
 *   <li><b>Verifying partial signatures</b> ({@code PartialVerify}) - Implemented by using
 *   {@link HintsLibrary#verifyBls(byte[], byte[], byte[], byte[])} with public keys extracted from the
 *   aggregation key in the active hinTS scheme via {@link HintsLibraryCodec#extractPublicKey(Bytes, int)}.</li>
 *   <li><b>Signature aggregation</b> ({@code SignAggr}) - Implemented by {@link HintsLibrary#aggregateSignatures(byte[], byte[], byte[], Map)}
 *   with partial signatures verified as above with weights extracted from the aggregation key in the active hinTS
 *   scheme via {@link HintsLibraryCodec#extractWeight(Bytes, int)} and {@link HintsLibraryCodec#extractTotalWeight(Bytes)}.</li>
 *   <li><b>Verifying aggregate signatures</b> ({@code Verify}) - Implemented by
 *   {@link HintsLibrary#verifyAggregate(byte[], byte[], byte[], byte[], long, long)}.</li>
 * </ul>
 */
public interface HintsLibrary {
    /**
     * Returns an initial CRS for the given number of parties.
     * @param n the number of parties
     * @return the CRS
     */
    byte[] newCrs(int n);

    /**
     * Updates the given CRS with the given 256 bits of entropy and returns the concatenation of the
     * updated CRS and a proof of the contribution.
     * @param crs the CRS
     * @param entropy the 256-bit entropy
     * @return the updated CRS and proof
     */
    byte[] updateCrs(@NonNull byte[] crs, @NonNull byte[] entropy);

    /**
     * Verifies the given proof of a CRS update.
     * @param oldCrs the old CRS
     * @param newCrs the new CRS
     * @param proof the proof
     * @return true if the proof is valid; false otherwise
     */
    boolean verifyCrsUpdate(@NonNull byte[] oldCrs, @NonNull byte[] newCrs, @NonNull byte[] proof);

    /**
     * Generates a new BLS key pair.
     * @return the key pair
     */
    byte[] newBlsKeyPair();

    /**
     * Computes the hints for the given public key and number of parties.
     *
     * @param crs
     * @param blsPrivateKey the private key
     * @param partyId       the party id
     * @param n             the number of parties
     * @return the hints
     */
    byte[] computeHints(final byte[] crs, @NonNull byte[] blsPrivateKey, int partyId, int n);

    /**
     * Validates the hinTS public key for the given number of parties.
     *
     * @param crs
     * @param hintsKey the hinTS key
     * @param partyId  the party id
     * @param n        the number of parties
     * @return true if the hints are valid; false otherwise
     */
    boolean validateHintsKey(final byte[] crs, @NonNull byte[] hintsKey, int partyId, int n);

    /**
     * Runs the hinTS preprocessing algorithm on the given validated hint keys and party weights for the given number
     * of parties. The output includes,
     * <ol>
     *     <li>The linear size aggregation key to use in combining partial signatures on a message with a provably
     *     well-formed aggregate public key.</li>
     *     <li>The succinct verification key to use when verifying an aggregate signature.</li>
     * </ol>
     * Both maps given must have the same key set; in particular, a subset of {@code [0, n)}.
     *
     * @param crs
     * @param hintsKeys the valid hinTS keys by party id
     * @param weights   the weights by party id
     * @param n         the number of parties
     * @return the preprocessed keys
     */
    AggregationAndVerificationKeys preprocess(
            final byte[] crs, @NonNull Map<Integer, Bytes> hintsKeys, @NonNull Map<Integer, Long> weights, int n);

    /**
     * Signs a message with a BLS private key.
     *
     * @param message the message
     * @param privateKey the private key
     * @return the signature
     */
    byte[] signBls(@NonNull byte[] message, @NonNull byte[] privateKey);

    /**
     * Checks that a signature on a message verifies under a BLS public key.
     *
     * @param crs
     * @param signature the signature
     * @param message   the message
     * @param publicKey the public key
     * @return true if the signature is valid; false otherwise
     */
    boolean verifyBls(final byte[] crs, @NonNull byte[] signature, @NonNull byte[] message, @NonNull byte[] publicKey);

    /**
     * Aggregates the signatures for party ids using hinTS aggregation and verification keys.
     *
     * @param crs
     * @param aggregationKey    the aggregation key
     * @param verificationKey   the verification key
     * @param partialSignatures the partial signatures by party id
     * @return the aggregated signature
     */
    byte[] aggregateSignatures(
            final byte[] crs,
            @NonNull byte[] aggregationKey,
            @NonNull byte[] verificationKey,
            @NonNull Map<Integer, Bytes> partialSignatures);

    /**
     * Checks an aggregate signature on a message verifies under a hinTS verification key, where
     * this is only true if the aggregate signature has weight exceeding the specified threshold
     * or total weight stipulated in the verification key.
     *
     * @param crs
     * @param signature            the aggregate signature
     * @param message              the message
     * @param verificationKey      the verification key
     * @param thresholdNumerator   the numerator of a fraction of total weight the signature must have
     * @param thresholdDenominator the denominator of a fraction of total weight the signature must have
     * @return true if the signature is valid; false otherwise
     */
    boolean verifyAggregate(
            final byte[] crs,
            @NonNull byte[] signature,
            @NonNull byte[] message,
            @NonNull byte[] verificationKey,
            long thresholdNumerator,
            long thresholdDenominator);
}
