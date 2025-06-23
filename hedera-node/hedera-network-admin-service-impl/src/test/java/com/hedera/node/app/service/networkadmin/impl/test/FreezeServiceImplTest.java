// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.networkadmin.impl.test;

import static com.hedera.node.app.service.networkadmin.impl.schemas.V0490FreezeSchema.FREEZE_TIME_KEY;
import static com.hedera.node.app.service.networkadmin.impl.schemas.V0490FreezeSchema.UPGRADE_FILE_HASH_KEY;
import static com.hedera.node.app.service.networkadmin.impl.schemas.V0640FreezeSchema.FREEZE_INFO_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.hedera.node.app.fixtures.state.FakeSchemaRegistry;
import com.hedera.node.app.fixtures.state.FakeState;
import com.hedera.node.app.service.networkadmin.FreezeService;
import com.hedera.node.app.service.networkadmin.impl.FreezeServiceImpl;
import com.swirlds.state.lifecycle.Schema;
import com.swirlds.state.lifecycle.SchemaRegistry;
import com.swirlds.state.lifecycle.StartupNetworks;
import com.swirlds.state.lifecycle.StateDefinition;
import com.swirlds.state.lifecycle.info.NetworkInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FreezeServiceImplTest {
    @Mock
    private SchemaRegistry registry;

    @Mock
    private NetworkInfo networkInfo;

    @Mock
    private StartupNetworks startupNetworks;

    @Test
    void testSpi() {
        // when
        final FreezeService service = new FreezeServiceImpl();

        // then
        assertNotNull(service, "We must always receive an instance");
        Assertions.assertEquals(
                FreezeServiceImpl.class,
                service.getClass(),
                "We must always receive an instance of type " + FreezeServiceImpl.class.getName());
    }

    @Test
    void registersExpectedSchema() {
        final var subject = new FreezeServiceImpl();
        ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);

        subject.registerSchemas(registry);
        verify(registry, times(2)).register(schemaCaptor.capture());
        final var schemas = schemaCaptor.getAllValues();

        final var statesToCreate0490 = schemas.get(0).statesToCreate();
        final var statesToCreate0640 = schemas.get(1).statesToCreate();
        assertEquals(2, statesToCreate0490.size(), "There should be 2 states created by the V0490FreezeSchema");
        assertEquals(1, statesToCreate0640.size(), "There should be 1 states created by the V640FreezeSchema");
        final var iter0490 = statesToCreate0490.stream()
                .map(StateDefinition::stateKey)
                .sorted()
                .iterator();
        final var iter0640 = statesToCreate0640.stream()
                .map(StateDefinition::stateKey)
                .sorted()
                .iterator();
        assertEquals(FREEZE_INFO_KEY, iter0640.next());
        assertEquals(FREEZE_TIME_KEY, iter0490.next());
        assertEquals(UPGRADE_FILE_HASH_KEY, iter0490.next());
    }

    @Test
    void migratesAsExpected() {
        final var subject = new FreezeServiceImpl();
        final var registry = new FakeSchemaRegistry();
        final var state = new FakeState();

        subject.registerSchemas(registry);
        registry.migrate(FreezeService.NAME, state, startupNetworks);
        final var upgradeFileHashKeyState =
                state.getReadableStates(FreezeService.NAME).getSingleton(UPGRADE_FILE_HASH_KEY);
        assertNull(upgradeFileHashKeyState.get());
    }
}
