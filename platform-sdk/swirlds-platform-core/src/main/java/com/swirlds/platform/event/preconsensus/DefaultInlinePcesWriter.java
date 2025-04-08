// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import com.swirlds.common.context.PlatformContext;
import com.swirlds.component.framework.model.WiringModel;
import com.swirlds.component.framework.schedulers.TaskScheduler;
import com.swirlds.component.framework.schedulers.builders.TaskSchedulerType;
import com.swirlds.component.framework.wires.input.BindableInputWire;
import com.swirlds.platform.event.preconsensus.CollectionPcesWriter.PrepareResult;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.EventWindow;
import org.hiero.consensus.model.node.NodeId;

public class DefaultInlinePcesWriter implements InlinePcesWriter2 {
    private final CollectionPcesWriter commonPcesWriter;
    private final NodeId selfId;
    private final FileSyncOption fileSyncOption;
    private final BlockingQueue<PlatformEvent> events = new LinkedBlockingQueue<>();
    private final TaskScheduler<List<PlatformEvent>> internalScheduler;
    private BindableInputWire<List<PlatformEvent>, List<PlatformEvent>> writeEvents;
    private AtomicBoolean started = new AtomicBoolean(false);
    /**
     * Constructor
     *
     * @param platformContext the platform context
     * @param fileManager     manages all preconsensus event stream files currently on disk
     */
    public DefaultInlinePcesWriter(
            @NonNull final PlatformContext platformContext,
            @NonNull final PcesFileManager fileManager,
            @NonNull final NodeId selfId) {
        Objects.requireNonNull(platformContext, "platformContext is required");
        Objects.requireNonNull(fileManager, "fileManager is required");
        commonPcesWriter = new CollectionPcesWriter(platformContext, fileManager, false);
        this.selfId = Objects.requireNonNull(selfId, "selfId is required");
        this.fileSyncOption = platformContext
                .getConfiguration()
                .getConfigData(PcesConfig.class)
                .inlinePcesSyncOption();
        internalScheduler = null;
    }

    public DefaultInlinePcesWriter(
            @NonNull final PlatformContext platformContext,
            @NonNull final PcesFileManager fileManager,
            @NonNull final NodeId selfId,
            WiringModel model,
            Consumer<PlatformEvent> inputWire) {
        Objects.requireNonNull(platformContext, "platformContext is required");
        Objects.requireNonNull(fileManager, "fileManager is required");
        commonPcesWriter = new CollectionPcesWriter(platformContext, fileManager, false);
        this.selfId = Objects.requireNonNull(selfId, "selfId is required");
        this.fileSyncOption = platformContext
                .getConfiguration()
                .getConfigData(PcesConfig.class)
                .inlinePcesSyncOption();
        this.internalScheduler = model.<List<PlatformEvent>>schedulerBuilder("DefaultInlinePcesWriterInternal")
                .withType(TaskSchedulerType.SEQUENTIAL_THREAD)
                .withFlushingEnabled(true)
                .withUnhandledTaskMetricEnabled(true)
                .withDataCounter(data -> 1)
                .build();

        this.writeEvents = internalScheduler.<List<PlatformEvent>>buildInputWire("writeEvents");
        writeEvents.bind(this::writeEvents);
        this.internalScheduler
                .getOutputWire()
                .<PlatformEvent>buildSplitter("split", "split")
                .solderTo("consumeEvent", "consumeEvent", inputWire);
    }

    @Override
    public void beginStreamingNewEvents() {
        commonPcesWriter.beginStreamingNewEvents();
    }

    @Nullable
    public PlatformEvent addEvent(@NonNull PlatformEvent event) {
        // if we aren't streaming new events yet, assume that the given event is already durable
        if (!commonPcesWriter.isStreamingNewEvents()) {
            return event;
        }

        if (commonPcesWriter.getFileType().selectIndicator(event) < commonPcesWriter.getNonAncientBoundary()) {
            // don't do anything with ancient events
            return event;
        }

        events.add(event);
        if (started.compareAndSet(false, true)) {
            final var data = new ArrayList<PlatformEvent>();
            events.drainTo(data);
            writeEvents.put(data);
        }
        return null;
    }

    @NonNull
    public List<PlatformEvent> writeEvents(@NonNull List<PlatformEvent> events) {
        try {

            PrepareResult result = commonPcesWriter.prepareOutputStream(events);
            commonPcesWriter.getCurrentMutableFile().writeEvents(result.eventsToWrite());

            // Sync always
            commonPcesWriter.getCurrentMutableFile().sync();
            final var newEvents = new ArrayList<PlatformEvent>();
            if (this.events.drainTo(newEvents) == 0) {
                // TODO:we need to block here to stop the active waiting or something. The challenge is how would we
                // stop this thread from running?
            } else {
                writeEvents.put(newEvents);
            }

            return events;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerDiscontinuity(@NonNull Long newOriginRound) {
        commonPcesWriter.registerDiscontinuity(newOriginRound);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNonAncientEventBoundary(@NonNull EventWindow nonAncientBoundary) {
        commonPcesWriter.updateNonAncientEventBoundary(nonAncientBoundary);
    }

    @Override
    public void setMinimumAncientIdentifierToStore(@NonNull final Long minimumAncientIdentifierToStore) {
        commonPcesWriter.setMinimumAncientIdentifierToStore(minimumAncientIdentifierToStore);
    }

    public void writeEvent(final PlatformEvent event) {
        writeEvents(List.of(event));
    }
}
