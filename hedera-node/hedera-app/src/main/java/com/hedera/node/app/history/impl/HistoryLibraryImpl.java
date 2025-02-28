// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.history.impl;

import static java.util.Objects.requireNonNull;

import com.hedera.cryptography.rpm.HistoryLibraryBridge;
import com.hedera.cryptography.rpm.ProvingAndVerifyingSnarkKeys;
import com.hedera.cryptography.rpm.SigningAndVerifyingSchnorrKeys;
import com.hedera.node.app.history.HistoryLibrary;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Default implementation of the {@link HistoryLibrary}.
 */
public class HistoryLibraryImpl implements HistoryLibrary {
    private static final SplittableRandom RANDOM = new SplittableRandom();
    private static final HistoryLibraryBridge BRIDGE = HistoryLibraryBridge.getInstance();
    private static final ProvingAndVerifyingSnarkKeys SNARK_KEYS;

    static {
        try {
            final var elf = HistoryLibraryBridge.loadAddressBookRotationProgram();
            SNARK_KEYS = BRIDGE.snarkVerificationKey(elf);
        } catch (IOException e) {
            throw new IllegalStateException("Could not load HistoryLibrary ELF", e);
        }
    }

    @Override
    public byte[] snarkVerificationKey() {
        return SNARK_KEYS.verifyingKey();
    }

    @Override
    public SigningAndVerifyingSchnorrKeys newSchnorrKeyPair() {
        final var bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return BRIDGE.newSchnorrKeyPair(bytes);
    }

    @Override
    public byte[] signSchnorr(@NonNull final byte[] message, @NonNull final byte[] privateKey) {
        requireNonNull(message);
        requireNonNull(privateKey);
        return BRIDGE.signSchnorr(message, privateKey);
    }

    @Override
    public boolean verifySchnorr(
            @NonNull final byte[] signature, @NonNull final byte[] message, @NonNull final byte[] publicKey) {
        requireNonNull(signature);
        requireNonNull(message);
        requireNonNull(publicKey);
        return BRIDGE.verifySchnorr(signature, message, publicKey);
    }

    @Override
    public byte[] hashAddressBook(@NonNull long[] weights, @NonNull byte[][] publicKeys) {
        requireNonNull(weights);
        requireNonNull(publicKeys);
        return BRIDGE.hashAddressBook(publicKeys, weights);
    }

    @Override
    public byte[] hashHintsVerificationKey(@NonNull final byte[] hintsVerificationKey) {
        return BRIDGE.hashHintsVerificationKey(hintsVerificationKey);
    }

    @NonNull
    @Override
    public byte[] proveChainOfTrust(
            @NonNull final byte[] ledgerId,
            @Nullable final byte[] sourceProof,
            @NonNull final long[] currentAddressBookWeights,
            @NonNull final byte[][] currentAddressBookVerifyingKeys,
            @NonNull final long[] nextAddressBookWeights,
            @NonNull final byte[][] nextAddressBookVerifyingKeys,
            @NonNull Map<Long, byte[]> sourceSignatures,
            @NonNull final byte[] targetMetadata) {
        requireNonNull(ledgerId);
        requireNonNull(currentAddressBookWeights);
        requireNonNull(currentAddressBookVerifyingKeys);
        requireNonNull(nextAddressBookWeights);
        requireNonNull(nextAddressBookVerifyingKeys);
        requireNonNull(sourceSignatures);
        requireNonNull(targetMetadata);

        final var verifyingSignatures = sourceSignatures.values().toArray(byte[][]::new);
        return BRIDGE.proveChainOfTrust(
                SNARK_KEYS.provingKey(),
                SNARK_KEYS.verifyingKey(),
                ledgerId,
                currentAddressBookVerifyingKeys,
                currentAddressBookWeights,
                nextAddressBookVerifyingKeys,
                nextAddressBookWeights,
                sourceProof,
                targetMetadata,
                verifyingSignatures);
    }

    @Override
    public boolean verifyChainOfTrust(
            @NonNull final byte[] ledgerId,
            @NonNull final byte[] addressBookHash,
            @NonNull final byte[] metadata,
            @NonNull final byte[] proof) {
        requireNonNull(ledgerId);
        requireNonNull(addressBookHash);
        requireNonNull(metadata);
        requireNonNull(proof);
        return BRIDGE.verifyChainOfTrust(SNARK_KEYS.verifyingKey(), proof);
    }
}
