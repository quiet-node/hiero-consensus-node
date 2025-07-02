// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.ids;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityNumGeneratorImplTest {

    @Mock
    private WritableEntityIdStore entityIdStore;

    private EntityNumGeneratorImpl subject;

    @BeforeEach
    void setup() {
        subject = new EntityNumGeneratorImpl(entityIdStore);
    }

    @Test
    void testNewEntityNumWithInitialState() {
        when(entityIdStore.incrementEntityNumAndGet()).thenReturn(1L);
        final var actual = subject.newEntityNum();

        assertThat(actual).isEqualTo(1L);
        verify(entityIdStore).incrementEntityNumAndGet();
    }

    @Test
    void testPeekingAtNewEntityNumWithInitialState() {
        when(entityIdStore.peekAtNextEntityNumber()).thenReturn(1L);
        final var actual = subject.peekAtNewEntityNum();

        assertThat(actual).isEqualTo(1L);

        verify(entityIdStore).peekAtNextEntityNumber();
    }

    @Test
    void testNewEntityNum() {
        when(entityIdStore.incrementEntityNumAndGet()).thenReturn(43L);

        final var actual = subject.newEntityNum();

        assertThat(actual).isEqualTo(43L);
        verify(entityIdStore).incrementEntityNumAndGet();
        verify(entityIdStore, never()).peekAtNextEntityNumber();
    }

    @Test
    void testPeekingAtNewEntityNum() {
        when(entityIdStore.peekAtNextEntityNumber()).thenReturn(43L);

        final var actual = subject.peekAtNewEntityNum();

        assertThat(actual).isEqualTo(43L);
        verify(entityIdStore).peekAtNextEntityNumber();
        verify(entityIdStore, never()).incrementEntityNumAndGet();
    }
}
