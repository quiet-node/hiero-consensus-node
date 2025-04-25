// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.contract.impl.nativelibverification;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.BDDMockito.given;

import com.hedera.node.config.data.ContractsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NativeLibVerifierTest {

    @Mock
    private ContractsConfig contractsConfig;

    private NativeLibVerifier verifier;

    @BeforeEach
    void setUp() {
        verifier = new NativeLibVerifier(() -> contractsConfig);
    }

    @Test
    void verifyNativeLibsInWarningMode() {
        given(contractsConfig.nativeLibVerificationHaltEnabled()).willReturn(false);
        assertDoesNotThrow(() -> verifier.verifyNativeLibs());
    }

    @Test
    void verifyNativeLibsInHaltMode() {
        given(contractsConfig.nativeLibVerificationHaltEnabled()).willReturn(true);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> verifier.verifyNativeLibs())
                .withMessageContaining("Native library");
    }
}
