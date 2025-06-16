// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.networkadmin.impl.test.schemas;

import static com.hedera.node.app.service.networkadmin.impl.schemas.V0640FreezeSchema.FREEZE_INFO_KEY;
import static com.hedera.node.app.service.networkadmin.impl.schemas.V0640FreezeSchema.FREEZE_TIME_KEY;
import static com.hedera.node.app.service.networkadmin.impl.schemas.V0640FreezeSchema.UPGRADE_FILE_HASH_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.blockstream.FreezeInfo;
import com.hedera.hapi.node.state.primitives.ProtoBytes;
import com.hedera.node.app.service.networkadmin.impl.schemas.V0640FreezeSchema;
import com.hedera.node.config.testfixtures.HederaTestConfigBuilder;
import com.swirlds.config.api.Configuration;
import com.swirlds.state.lifecycle.MigrationContext;
import com.swirlds.state.lifecycle.StateDefinition;
import com.swirlds.state.spi.WritableSingletonState;
import com.swirlds.state.spi.WritableStates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class V0640FreezeSchemaTest {
    private static final Configuration DEFAULT_CONFIG = HederaTestConfigBuilder.createConfig();

    @Mock
    private MigrationContext migrationContext;

    @Mock
    private WritableStates writableStates;

    @Mock
    private WritableSingletonState<FreezeInfo> freezeInfoState;

    @Mock
    private WritableSingletonState<ProtoBytes> upgradeFileHashState;

    @Mock
    private WritableSingletonState<Timestamp> freezeTimeState;

    private V0640FreezeSchema subject;

    @BeforeEach
    void setUp() {
        subject = new V0640FreezeSchema();
    }

    @Test
    void registersExpectedSchema() {
        final var statesToCreate = subject.statesToCreate();
        assertThat(statesToCreate.size()).isEqualTo(3);
        final var iter =
                statesToCreate.stream().map(StateDefinition::stateKey).sorted().iterator();
        assertEquals(FREEZE_INFO_KEY, iter.next());
        assertEquals(FREEZE_TIME_KEY, iter.next());
        assertEquals(UPGRADE_FILE_HASH_KEY, iter.next());
    }

    @Test
    void genesisFreezeInfoIsPopulatedInState() {
        given(migrationContext.newStates()).willReturn(writableStates);
        given(writableStates.<FreezeInfo>getSingleton(FREEZE_INFO_KEY)).willReturn(freezeInfoState);
        given(writableStates.<Timestamp>getSingleton(FREEZE_TIME_KEY)).willReturn(freezeTimeState);
        given(writableStates.<ProtoBytes>getSingleton(UPGRADE_FILE_HASH_KEY)).willReturn(upgradeFileHashState);
        given(migrationContext.isGenesis()).willReturn(true);

        subject.restart(migrationContext);

        verify(freezeInfoState).put(FreezeInfo.newBuilder().lastFreezeRound(0L).build());
        verify(freezeTimeState).put(Timestamp.DEFAULT);
        verify(upgradeFileHashState).put(ProtoBytes.DEFAULT);
    }

    @Test
    void migrationFreezeInfoIsNotInState() {
        given(migrationContext.appConfig()).willReturn(DEFAULT_CONFIG);
        given(migrationContext.newStates()).willReturn(writableStates);
        given(writableStates.<FreezeInfo>getSingleton(FREEZE_INFO_KEY)).willReturn(freezeInfoState);
        given(migrationContext.isGenesis()).willReturn(false);
        given(freezeInfoState.get()).willReturn(null);
        given(migrationContext.isUpgrade(any())).willReturn(true);
        given(migrationContext.roundNumber()).willReturn(42L);

        subject.restart(migrationContext);

        verify(freezeInfoState).put(FreezeInfo.newBuilder().lastFreezeRound(42L).build());
    }

    @Test
    void migrationFreezeInfoIsInState() {
        given(migrationContext.appConfig()).willReturn(DEFAULT_CONFIG);
        given(migrationContext.newStates()).willReturn(writableStates);
        given(writableStates.<FreezeInfo>getSingleton(FREEZE_INFO_KEY)).willReturn(freezeInfoState);
        given(migrationContext.isGenesis()).willReturn(false);
        given(freezeInfoState.get())
                .willReturn(FreezeInfo.newBuilder().lastFreezeRound(9000L).build());
        given(migrationContext.isUpgrade(any())).willReturn(false);

        subject.restart(migrationContext);
        verify(freezeInfoState).get();
        verifyNoMoreInteractions(freezeInfoState);
    }
}
