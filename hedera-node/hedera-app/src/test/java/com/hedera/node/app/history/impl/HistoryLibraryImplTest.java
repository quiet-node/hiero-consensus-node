// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.history.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.cryptography.rpm.SigningAndVerifyingSchnorrKeys;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HistoryLibraryImplTest {
    private static final HistoryLibraryImpl subject = new HistoryLibraryImpl();

    @Test
    void hasSnarkVerificationKey() {
        assertNotNull(subject.snarkVerificationKey());
    }

    @Test
    void generatesValidSchnorrKeys() {
        final var keys = subject.newSchnorrKeyPair();
        final var message = "Hello, world!".getBytes();
        final var signature = subject.signSchnorr(message, keys.signingKey());
        assertTrue(subject.verifySchnorr(signature, message, keys.verifyingKey()));
    }

    @Test
    void hashesAddressBook() {
        final List<KeyPairAndWeight> addresses = buildSomeAddresses(3);
        final var publicKeyArray =
                addresses.stream().map(k -> k.keys.verifyingKey()).toArray(byte[][]::new);
        final var weights = addresses.stream()
                .map(KeyPairAndWeight::weight)
                .mapToLong(Long::longValue)
                .toArray();
        assertNotNull(subject.hashAddressBook(weights, publicKeyArray));
    }

    @Test
    void verifiesProofOfTrust() {
        final List<KeyPairAndWeight> sourceAddresses = buildSomeAddresses(3);
        final var sourceKeys =
                sourceAddresses.stream().map(k -> k.keys.verifyingKey()).toArray(byte[][]::new);
        final var sourceWeights = sourceAddresses.stream()
                .map(KeyPairAndWeight::weight)
                .mapToLong(Long::longValue)
                .toArray();

        final List<KeyPairAndWeight> targetAddresses = buildSomeAddresses(2);
        final var targetKeys =
                targetAddresses.stream().map(k -> k.keys.verifyingKey()).toArray(byte[][]::new);
        final var targetWeights = targetAddresses.stream()
                .map(KeyPairAndWeight::weight)
                .mapToLong(Long::longValue)
                .toArray();

        final byte[] genesisAddressBookHash = subject.hashAddressBook(sourceWeights, sourceKeys);
        final byte[] nextAddressBookHash = subject.hashAddressBook(targetWeights, targetKeys);
        final byte[] metadata =
                com.hedera.pbj.runtime.io.buffer.Bytes.wrap("test metadata").toByteArray();
        final var hashedMetadata = subject.hashHintsVerificationKey(metadata);

        final var message = concatMessages(nextAddressBookHash, hashedMetadata);

        final Map<Long, byte[]> signatures = new LinkedHashMap<>();

        for (var entry : sourceAddresses) {
            signatures.put(entry.weight(), subject.signSchnorr(message, entry.keys.signingKey()));
        }

        final var snarkProof = subject.proveChainOfTrust(
                genesisAddressBookHash,
                null,
                sourceWeights,
                sourceKeys,
                targetWeights,
                targetKeys,
                signatures,
                hashedMetadata);
        assertNotNull(snarkProof);
    }

    public static byte[] concatMessages(final byte[] nextAddressBookHash, final byte[] hintsVerificationKeyHash) {
        final byte[] arr = new byte[nextAddressBookHash.length + hintsVerificationKeyHash.length];
        System.arraycopy(nextAddressBookHash, 0, arr, 0, nextAddressBookHash.length);
        System.arraycopy(hintsVerificationKeyHash, 0, arr, nextAddressBookHash.length, hintsVerificationKeyHash.length);
        return arr;
    }

    private record KeyPairAndWeight(SigningAndVerifyingSchnorrKeys keys, long weight) {}

    private KeyPairAndWeight fromRandom(long weight) {
        final SigningAndVerifyingSchnorrKeys keys = subject.newSchnorrKeyPair();
        return new KeyPairAndWeight(keys, weight);
    }

    private List<KeyPairAndWeight> buildSomeAddresses(final int num) {
        return List.of(fromRandom(111), fromRandom(222), fromRandom(333), fromRandom(444))
                .subList(0, num);
    }
}
