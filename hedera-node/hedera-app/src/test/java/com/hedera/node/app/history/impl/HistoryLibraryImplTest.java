// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.history.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.primitives.Bytes;
import com.hedera.cryptography.rpm.SigningAndVerifyingSchnorrKeys;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
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
        final List<KeyPairAndWeight> sourceAddresses = buildSomeAddresses(4);
        final var sourceKeys =
                sourceAddresses.stream().map(k -> k.keys.verifyingKey()).toArray(byte[][]::new);
        final var sourceWeights = sourceAddresses.stream()
                .map(KeyPairAndWeight::weight)
                .mapToLong(Long::longValue)
                .toArray();

        final List<KeyPairAndWeight> targetAddresses = buildSomeAddresses(3);
        final var targetKeys =
                targetAddresses.stream().map(k -> k.keys.verifyingKey()).toArray(byte[][]::new);
        final var targetWeights = targetAddresses.stream()
                .map(KeyPairAndWeight::weight)
                .mapToLong(Long::longValue)
                .toArray();

        final byte[] genesisAddressBookHash = subject.hashAddressBook(sourceWeights, sourceKeys);
        final var snarkVerificationKey = subject.snarkVerificationKey();
        final var ledgerId = Bytes.concat(genesisAddressBookHash, snarkVerificationKey);

        final var message = "Hello, world!".getBytes();
        final Map<Long, byte[]> signatures = sourceAddresses.stream()
                .map(kp -> Pair.of(kp.weight, subject.signSchnorr(message, kp.keys.signingKey())))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

        final var snarkProof = subject.proveChainOfTrust(
                ledgerId, null, sourceWeights, sourceKeys, targetWeights, targetKeys, signatures, message);
        assertNotNull(snarkProof);
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
