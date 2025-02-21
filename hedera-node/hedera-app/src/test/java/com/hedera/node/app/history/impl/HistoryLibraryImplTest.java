// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.history.impl;

import static org.junit.jupiter.api.Assertions.*;

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
}
