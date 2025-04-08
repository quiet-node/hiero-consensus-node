// SPDX-License-Identifier: Apache-2.0
package com.swirlds.component.framework.wires.input;

import static com.swirlds.component.framework.schedulers.builders.TaskSchedulerType.NO_OP;

import com.swirlds.component.framework.model.TraceableWiringModel;
import com.swirlds.component.framework.schedulers.TaskScheduler;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An input wire that can be bound to an implementation.
 *
 * @param <IN>  the type of data that passes into the wire
 * @param <OUT> the type of the primary output wire for the scheduler that is associated with this object
 */
public class CollectableInputWire<IN, OUT> extends InputWire<IN> {

    private final String taskSchedulerName;

    /**
     * Supplier for whether the task scheduler is currently squelching.
     * <p>
     * As long as this supplier returns true, the handler will be executed as a no-op, and no data will be forwarded.
     */
    private final Supplier<Boolean> currentlySquelching;

    /**
     * True if this is a wire on a no-op scheduler.
     */
    private final boolean noOp;

    /**
     * Constructor.
     *
     * @param model         the wiring model containing this input wire
     * @param taskScheduler the scheduler to insert data into
     * @param name          the name of the input wire
     */
    public CollectableInputWire(
            @NonNull final TraceableWiringModel model,
            @NonNull final TaskScheduler<OUT> taskScheduler,
            @NonNull final String name,
            Function<List<IN>, List<OUT>> handler) {
        super(taskScheduler, name);
        taskSchedulerName = taskScheduler.getName();
        currentlySquelching = taskScheduler::currentlySquelching;

        noOp = taskScheduler.getType() == NO_OP;

        if (noOp) {
            return;
        }
        model.registerInputWireCreation(taskSchedulerName, name);
        setHandler(new HackHandler<>(currentlySquelching, Objects.requireNonNull(taskScheduler), handler));
        Objects.requireNonNull(model).registerInputWireBinding(taskSchedulerName, getName());
    }

    public record HackHandler<IN, OUT>(
            Supplier<Boolean> currentlySquelching,
            TaskSchedulerInput<OUT> taskSchedulerInput,
            Function<List<IN>, List<OUT>> handler)
            implements Consumer<Object> {

        @SuppressWarnings("unchecked")
        public void accept(Object i) {
            if (currentlySquelching.get()) {
                return;
            }

            final List<OUT> output = handler.apply((List<IN>) i);
            if (output != null) {
                for (OUT a : output) {
                    taskSchedulerInput.forward(a);
                }
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof final HackHandler<?, ?> that)) {
                return false;
            }
            return Objects.equals(handler, that.handler);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(handler);
        }
    }
}
