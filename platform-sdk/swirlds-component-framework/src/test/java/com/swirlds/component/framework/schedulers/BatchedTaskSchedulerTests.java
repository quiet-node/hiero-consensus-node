// SPDX-License-Identifier: Apache-2.0
package com.swirlds.component.framework.schedulers;

import static com.swirlds.component.framework.schedulers.builders.TaskSchedulerBuilder.UNLIMITED_CAPACITY;

import com.swirlds.component.framework.TestWiringModelBuilder;
import com.swirlds.component.framework.model.WiringModel;
import com.swirlds.component.framework.schedulers.builders.TaskSchedulerType;
import com.swirlds.component.framework.schedulers.internal.BatchedTaskScheduler;
import com.swirlds.component.framework.wires.input.BindableInputWire;
import com.swirlds.component.framework.wires.input.CollectableInputWire;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class BatchedTaskSchedulerTests {

    /**
     * Add values to the task scheduler, ensure that each value was processed in the correct order.
     */
    @Test
    void orderOfOperationsTest() throws InterruptedException {
        final WiringModel model = TestWiringModelBuilder.create();

        final Function<List<Integer>, List<Integer>> handler = x -> {
            System.out.println(x);
            return x;
        };

        final TaskScheduler<Integer> taskScheduler = model.<Integer>schedulerBuilder("test")
                .withType(TaskSchedulerType.BATCH)
                .withUnhandledTaskCapacity(UNLIMITED_CAPACITY)
                .build();

        taskScheduler.getOutputWire().solderTo("name", "name", System.out::println);
        final var scheduler = ((BatchedTaskScheduler<Integer>) taskScheduler);

        final CollectableInputWire<Integer, Integer> channel = scheduler.buildCollectableInputWire("name", handler);
        final BindableInputWire<Integer, Integer> channel2 = taskScheduler.buildInputWire("name2");
        channel2.bind(x -> x);

        model.start();
        for (int i = 0; i < 100; i++) {
            channel.put(i);
        }

        channel2.put(-1);
        Thread.sleep(1000);
        model.stop();
    }
}
