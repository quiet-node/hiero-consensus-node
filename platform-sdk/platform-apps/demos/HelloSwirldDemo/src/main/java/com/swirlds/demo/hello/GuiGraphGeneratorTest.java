package com.swirlds.demo.hello;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hiero.base.crypto.Hash;

class GuiGraphGeneratorTest {

    public static void main(final String[] args) {
        try {
            basicTest();
            System.out.println("-----------");
            System.out.println("TEST PASSED");
            System.out.println("-----------");
        }catch (final Exception e){
            e.printStackTrace(System.out);
            System.out.println("-----------");
            System.out.println("TEST FAILED");
            System.out.println("-----------");
        }
    }


    private static void basicTest(){
        final GuiGraphGenerator generator = new GuiGraphGenerator(1, 10);

        final Set<Bytes> returnedEvents = new HashSet<>();

        while (true){
            final List<GuiEvent> guiEvents = generator.generateEvents(1);
            assertFalse(guiEvents.isEmpty());
            if(guiEvents.size() == 1){
                returnedEvents.add(guiEvents.getFirst().id());
                continue;
            }
            int notRemoved = 0;
            for (final GuiEvent guiEvent : guiEvents) {
                final boolean removed = returnedEvents.remove(guiEvent.id());
                if(!removed){
                    notRemoved++;
                }
            }
            assertEquals(1, notRemoved);
            break;
        }
    }
}