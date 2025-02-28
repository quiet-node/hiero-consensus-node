// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.hints.impl;

import static java.util.Objects.requireNonNull;

import com.hedera.cryptography.hints.AggregationAndVerificationKeys;
import com.hedera.cryptography.hints.HintsLibraryBridge;
import com.hedera.node.app.hints.HintsLibrary;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Default implementation of {@link HintsLibrary} (all TODO).
 */
public class HintsLibraryImpl implements HintsLibrary {
    private static final SplittableRandom RANDOM = new SplittableRandom();
    private static final HintsLibraryBridge BRIDGE = HintsLibraryBridge.getInstance();

    @Override
    public byte[] newCrs(final int n) {
        return BRIDGE.initCRS(n);
    }

    @Override
    public byte[] updateCrs(@NonNull final byte[] crs, @NonNull final byte[] entropy) {
        requireNonNull(crs);
        requireNonNull(entropy);
        return BRIDGE.updateCRS(crs, entropy);
    }

    @Override
    public boolean verifyCrsUpdate(@NonNull byte[] oldCrs, @NonNull byte[] newCrs, @NonNull byte[] proof) {
        requireNonNull(oldCrs);
        requireNonNull(newCrs);
        requireNonNull(proof);
        return BRIDGE.verifyCRS(oldCrs, newCrs, proof);
    }

    @Override
    public byte[] newBlsKeyPair() {
        final byte[] randomBytes = new byte[32];
        RANDOM.nextBytes(randomBytes);
        return BRIDGE.generateSecretKey(randomBytes);
    }

    @Override
    public byte[] computeHints(
            @NonNull final byte[] crs, @NonNull final byte[] blsPrivateKey, final int partyId, final int n) {
        requireNonNull(blsPrivateKey);
        return BRIDGE.computeHints(crs, blsPrivateKey, partyId, n);
    }

    @Override
    public boolean validateHintsKey(
            @NonNull final byte[] crs, @NonNull final byte[] hintsKey, final int partyId, final int n) {
        requireNonNull(crs);
        requireNonNull(hintsKey);
        return BRIDGE.validateHintsKey(crs, hintsKey, partyId, n);
    }

    @Override
    public AggregationAndVerificationKeys preprocess(
            @NonNull final byte[] crs,
            @NonNull final Map<Integer, Bytes> hintsKeys,
            @NonNull final Map<Integer, Long> weights,
            final int n) {
        requireNonNull(crs);
        requireNonNull(hintsKeys);
        requireNonNull(weights);
        final int[] parties =
                hintsKeys.keySet().stream().mapToInt(Integer::intValue).toArray();
        final byte[][] hintsPublicKeys =
                hintsKeys.values().stream().map(Bytes::toByteArray).toArray(byte[][]::new);
        final long[] weightsArray =
                weights.values().stream().mapToLong(Long::longValue).toArray();
        return BRIDGE.preprocess(crs, parties, hintsPublicKeys, weightsArray, n);
    }

    @Override
    public byte[] signBls(@NonNull final byte[] message, @NonNull final byte[] privateKey) {
        requireNonNull(message);
        requireNonNull(privateKey);
        return BRIDGE.signBls(message, privateKey);
    }

    @Override
    public boolean verifyBls(
            @NonNull final byte[] crs,
            @NonNull final byte[] signature,
            @NonNull final byte[] message,
            @NonNull final byte[] publicKey) {
        requireNonNull(crs);
        requireNonNull(signature);
        requireNonNull(message);
        requireNonNull(publicKey);
        return BRIDGE.verifyBls(crs, signature, message, publicKey);
    }

    @Override
    public byte[] aggregateSignatures(
            @NonNull final byte[] crs,
            @NonNull final byte[] aggregationKey,
            @NonNull final byte[] verificationKey,
            @NonNull final Map<Integer, byte[]> partialSignatures) {
        requireNonNull(crs);
        requireNonNull(aggregationKey);
        requireNonNull(verificationKey);
        requireNonNull(partialSignatures);
        final int[] parties =
                partialSignatures.keySet().stream().mapToInt(Integer::intValue).toArray();
        final byte[][] signatures = partialSignatures.values().toArray(byte[][]::new);
        return BRIDGE.aggregateSignatures(crs, aggregationKey, verificationKey, parties, signatures);
    }

    @Override
    public boolean verifyAggregate(
            @NonNull final byte[] crs,
            @NonNull final byte[] signature,
            @NonNull final byte[] message,
            @NonNull final byte[] verificationKey,
            final long thresholdNumerator,
            long thresholdDenominator) {
        requireNonNull(crs);
        requireNonNull(signature);
        requireNonNull(message);
        requireNonNull(verificationKey);
        return BRIDGE.verifyAggregate(
                crs, signature, message, verificationKey, thresholdNumerator, thresholdDenominator);
    }
}
