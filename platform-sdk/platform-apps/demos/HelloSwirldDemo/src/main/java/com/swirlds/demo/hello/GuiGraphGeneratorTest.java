package com.swirlds.demo.hello;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        Assertions.assertNotNull(generator.generateEvents(1));
    }
}