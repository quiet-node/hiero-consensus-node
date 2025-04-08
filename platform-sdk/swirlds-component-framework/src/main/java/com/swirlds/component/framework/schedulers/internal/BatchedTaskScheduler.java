// SPDX-License-Identifier: Apache-2.0
package com.swirlds.component.framework.schedulers.internal;

import com.swirlds.base.state.Startable;
import com.swirlds.base.state.Stoppable;
import com.swirlds.common.metrics.extensions.FractionalTimer;
import com.swirlds.component.framework.counters.ObjectCounter;
import com.swirlds.component.framework.model.TraceableWiringModel;
import com.swirlds.component.framework.schedulers.TaskScheduler;
import com.swirlds.component.framework.schedulers.builders.TaskSchedulerType;
import com.swirlds.component.framework.wires.input.CollectableInputWire;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A scheduler that performs work sequentially on a dedicated thread. This class has very similar semantics to
 * {@link DirectTaskScheduler}, except that work is done on a thread instead of on a fork join pool.
 *
 * @param <OUT> the type of the primary output wire
 */
public class BatchedTaskScheduler<OUT> extends TaskScheduler<OUT> implements Startable, Stoppable {

    /**
     * This task is meant to unblock tasks list take() operation in case the stop signal is received.
     */
    private static final Object SIGNAL = "PoisonPill";

    public static final String THREAD_NAME_PREFIX = "<scheduler ";
    public static final String THREAD_NAME_SUFFIX = ">";
    private final UncaughtExceptionHandler uncaughtExceptionHandler;
    private final ObjectCounter onRamp;
    private final ObjectCounter offRamp;
    private final ToLongFunction<Object> dataCounter;
    private final FractionalTimer busyTimer;
    private final long capacity;

    private final ConcurrentHashMap<Consumer<Object>, BlockingQueue<BatchThreadTask>> tasks = new ConcurrentHashMap<>();
    BlockingQueue<Object> signalQueue = new LinkedBlockingQueue<>();
    private static final int BUFFER_SIZE = 1024;

    private volatile boolean alive = true;

    private final Thread thread;

    /**
     * Constructor.
     *
     * @param model                    the wiring model containing this task scheduler
     * @param name                     the name of the task scheduler
     * @param uncaughtExceptionHandler the handler to call when an exception is thrown by a task. In this scheduler, the
     *                                 handler is executed immediately after the task that throws the exception.
     * @param onRamp                   the counter to increment when a task is added to the queue
     * @param offRamp                  the counter to decrement when a task is removed from the queue
     * @param dataCounter              the function to weight input data objects for health monitoring
     * @param busyTimer                the timer to activate when a task is being handled
     * @param capacity                 the maximum desired capacity for this task scheduler
     * @param flushEnabled             if true, then {@link #flush()} will be enabled, otherwise it will throw.
     * @param squelchingEnabled        if true, then squelching will be enabled, otherwise trying to squelch will throw
     * @param insertionIsBlocking      when data is inserted into this task scheduler, will it block until capacity is
     *                                 available?
     */
    public BatchedTaskScheduler(
            @NonNull final TraceableWiringModel model,
            @NonNull final String name,
            @NonNull final UncaughtExceptionHandler uncaughtExceptionHandler,
            @NonNull final ObjectCounter onRamp,
            @NonNull final ObjectCounter offRamp,
            @NonNull final ToLongFunction<Object> dataCounter,
            @NonNull final FractionalTimer busyTimer,
            final long capacity,
            final boolean flushEnabled,
            final boolean squelchingEnabled,
            final boolean insertionIsBlocking) {
        super(model, name, TaskSchedulerType.BATCH, flushEnabled, squelchingEnabled, insertionIsBlocking);

        this.uncaughtExceptionHandler = Objects.requireNonNull(uncaughtExceptionHandler);
        this.onRamp = Objects.requireNonNull(onRamp);
        this.offRamp = Objects.requireNonNull(offRamp);
        this.dataCounter = dataCounter;
        this.busyTimer = Objects.requireNonNull(busyTimer);
        this.capacity = capacity;

        thread = new Thread(this::run, THREAD_NAME_PREFIX + name + THREAD_NAME_SUFFIX);
    }

    @NonNull
    public <I> CollectableInputWire<I, OUT> buildCollectableInputWire(
            @NonNull final String name, Function<List<I>, List<OUT>> handler) {
        return new CollectableInputWire<>(super.model, this, name, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getUnprocessedTaskCount() {
        return onRamp.getCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCapacity() {
        return capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
        throwIfFlushDisabled();
        onRamp.waitUntilEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void put(@NonNull final Consumer<Object> handler, @NonNull final Object data) {
        onRamp.onRamp(dataCounter.applyAsLong(data));
        final BatchThreadTask e = new BatchThreadTask(data);
        tasks.computeIfPresent(handler, (x, y) -> {
            y.add(e);
            return y;
        });
        tasks.putIfAbsent(handler, new LinkedBlockingQueue<>(List.of(e)));
        signalQueue.add(SIGNAL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean offer(@NonNull final Consumer<Object> handler, @NonNull final Object data) {
        final boolean accepted = onRamp.attemptOnRamp(dataCounter.applyAsLong(data));
        if (!accepted) {
            return false;
        }
        final BatchThreadTask e = new BatchThreadTask(data);
        tasks.computeIfPresent(handler, (x, y) -> {
            y.add(e);
            return y;
        });
        tasks.putIfAbsent(handler, new LinkedBlockingQueue<>(List.of(e)));
        signalQueue.add(SIGNAL);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void inject(@NonNull final Consumer<Object> handler, @NonNull final Object data) {
        onRamp.forceOnRamp(dataCounter.applyAsLong(data));
        final BatchThreadTask e = new BatchThreadTask(data);
        tasks.computeIfPresent(handler, (x, y) -> {
            y.add(e);
            return y;
        });
        tasks.putIfAbsent(handler, new LinkedBlockingQueue<>(List.of(e)));
        signalQueue.add(SIGNAL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        thread.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        alive = false;
        signalQueue.add(SIGNAL);
    }

    /**
     * Take work off of the queue and handle it.
     */
    private void run() {

        while (alive) {
            try {
                signalQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            final var keys = new ArrayList<>(tasks.keySet());
            Collections.shuffle(keys); // random sort the keys

            final List<BatchThreadTask> buffer = new ArrayList<>(BUFFER_SIZE);
            for (Consumer<Object> key : keys) {
                final BlockingQueue<BatchThreadTask> value = tasks.get(key);
                if (value.drainTo(buffer, BUFFER_SIZE) == 0) {
                    continue;
                }
                if (!alive) {
                    break;
                }
                busyTimer.activate();
                try {
                    if (key instanceof CollectableInputWire.HackHandler<?, ?>) {
                        key.accept(buffer.stream().map(BatchThreadTask::data).toList());
                    } else {
                        for (var val : buffer) {
                            key.accept(val.data);
                        }
                    }
                } catch (final Throwable t) {
                    uncaughtExceptionHandler.uncaughtException(thread, t);
                } finally {
                    for (var t : buffer) {
                        offRamp.offRamp(dataCounter.applyAsLong(t));
                    }
                }

                busyTimer.deactivate();
                buffer.clear();
            }
        }
        System.out.println("Shutdown");
    }

    record BatchThreadTask(@NonNull Object data) {}
}
