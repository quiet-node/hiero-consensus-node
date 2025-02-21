// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.history.impl;

import static java.util.Objects.requireNonNull;

import com.hedera.node.app.history.HistoryLibrary;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;

/**
 * Default implementation of the {@link HistoryLibrary}.
 */
public class HistoryLibraryImpl implements HistoryLibrary {
    @Override
    public byte[] snarkVerificationKey() {
        throw new AssertionError("Not implemented");
    }

    @Override
    public byte[] newSchnorrKeyPair() {
        throw new AssertionError("Not implemented");
    }

    @Override
    public byte[] signSchnorr(@NonNull final byte[] message, @NonNull final byte[] privateKey) {
        requireNonNull(message);
        requireNonNull(privateKey);
        throw new AssertionError("Not implemented");
    }

    @Override
    public boolean verifySchnorr(
            @NonNull final byte[] signature, @NonNull final byte[] message, @NonNull final byte[] publicKey) {
        requireNonNull(signature);
        requireNonNull(message);
        requireNonNull(publicKey);
        throw new AssertionError("Not implemented");
    }

    @Override
    public byte[] hashAddressBook(@NonNull final byte[] addressBook) {
        requireNonNull(addressBook);
        throw new AssertionError("Not implemented");
    }

    @NonNull
    @Override
    public byte[] proveChainOfTrust(
            @NonNull final byte[] ledgerId,
            @Nullable final byte[] sourceProof,
            @NonNull final byte[] sourceAddressBook,
            @NonNull Map<Long, byte[]> sourceSignatures,
            @NonNull final byte[] targetAddressBookHash,
            @NonNull final byte[] targetMetadata) {
        requireNonNull(ledgerId);
        requireNonNull(sourceAddressBook);
        requireNonNull(sourceSignatures);
        requireNonNull(targetAddressBookHash);
        requireNonNull(targetMetadata);
        throw new AssertionError("Not implemented");
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
        throw new AssertionError("Not implemented");
    }
}
