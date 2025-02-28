// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.hints.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HintsLibraryImplTest {
    private final HintsLibraryImpl subject = new HintsLibraryImpl();

    @Test
    void nothingSupportedYet() {
        assertThrows(UnsupportedOperationException.class, () -> subject.newCrs(1));
        assertThrows(UnsupportedOperationException.class, () -> subject.updateCrs(new byte[0], new byte[0]));
        assertThrows(
                UnsupportedOperationException.class,
                () -> subject.verifyCrsUpdate(new byte[0], new byte[0],new byte[0]));

        assertThrows(UnsupportedOperationException.class, subject::newBlsKeyPair);
        assertThrows(UnsupportedOperationException.class, () -> subject.computeHints(new byte[0], new byte[0], 1, 1));
        assertThrows(UnsupportedOperationException.class, () -> subject.validateHintsKey(new byte[0], new byte[0], 1, 1));
        assertThrows(UnsupportedOperationException.class, () -> subject.preprocess(new byte[0], Map.of(), Map.of(), 1));
        assertThrows(UnsupportedOperationException.class, () -> subject.signBls(new byte[0], new byte[0]));
        assertThrows(
                UnsupportedOperationException.class, () -> subject.verifyBls(new byte[0], new byte[0], new byte[0], new byte[0]));
        assertThrows(
                UnsupportedOperationException.class,
                () -> subject.aggregateSignatures(new byte[0], new byte[0], new byte[0], Map.of()));
    }
}
