// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.gui;

import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.io.ResourceLoader;
import com.swirlds.common.test.fixtures.platform.TestPlatformContextBuilder;
import com.swirlds.config.extensions.test.fixtures.TestConfigBuilder;
import com.swirlds.platform.config.PathsConfig_;
import com.swirlds.platform.config.legacy.LegacyConfigProperties;
import com.swirlds.platform.config.legacy.LegacyConfigPropertiesLoader;
import com.swirlds.platform.event.AncientMode;
import com.swirlds.platform.event.PlatformEvent;
import com.swirlds.platform.event.preconsensus.PcesFileReader;
import com.swirlds.platform.event.preconsensus.PcesFileTracker;
import com.swirlds.platform.event.preconsensus.PcesMultiFileIterator;
import com.swirlds.platform.event.preconsensus.PcesUtilities;
import com.swirlds.platform.test.fixtures.event.generator.GraphGenerator;
import com.swirlds.platform.test.fixtures.event.generator.StandardGraphGenerator;
import com.swirlds.platform.test.fixtures.event.source.EventSource;
import com.swirlds.platform.test.fixtures.event.source.StandardEventSource;
import com.swirlds.platform.test.fixtures.gui.ListEventProvider;
import com.swirlds.platform.test.fixtures.gui.TestGuiSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// Used to replicate coin round issue
public class HashgraphGuiTest {

    @TempDir
    public Path tempDir;

    public static final String TEST_MARKER_FILE_DIRECTORY = "marker_files";

    @ParameterizedTest
    @ValueSource(strings = {"coin-round-test/0.60-20250225-114602/"})
    void runGuiWithControls(final String resources) throws URISyntaxException, IOException {
        final Randotron randotron = Randotron.create(1);
        final int numNodes = 4;

        final PlatformContext platformContext =
                TestPlatformContextBuilder.create().build();

        final GraphGenerator graphGenerator =
                new StandardGraphGenerator(platformContext, randotron.nextInt(), generateSources(numNodes));
        graphGenerator.reset();

        final List<PlatformEvent> platformEvents = new ArrayList<>();

        final PlatformContext context = createDefaultPlatformContext();

        final Path dir = ResourceLoader.getFile(resources + "events");
        PcesUtilities.compactPreconsensusEventFiles(dir);

        final PcesFileTracker pcesFileTracker =
                PcesFileReader.readFilesFromDisk(context, dir, 0, false, AncientMode.GENERATION_THRESHOLD);
        final PcesMultiFileIterator eventIterator = pcesFileTracker.getEventIterator(0, 0);
        final LegacyConfigProperties legacyConfigProperties =
                LegacyConfigPropertiesLoader.loadConfigFile(ResourceLoader.getFile(resources + "config.txt"));
        final com.swirlds.platform.test.fixtures.consensus.TestIntake intake =
                new com.swirlds.platform.test.fixtures.consensus.TestIntake(
                        context, legacyConfigProperties.getAddressBook());
        while (eventIterator.hasNext()) {
            final PlatformEvent event = eventIterator.next();
            intake.addEvent(event);
            platformEvents.add(event);
        }

        final TestGuiSource guiSource = new TestGuiSource(
                platformContext, legacyConfigProperties.getAddressBook(), new ListEventProvider(platformEvents));
        guiSource.runGui();
    }

    protected PlatformContext createDefaultPlatformContext() {
        return createPlatformContext(null, null);
    }

    protected PlatformContext createPlatformContext(
            @Nullable final Function<TestPlatformContextBuilder, TestPlatformContextBuilder> platformContextModifier,
            @Nullable final Function<TestConfigBuilder, TestConfigBuilder> configModifier) {
        final TestPlatformContextBuilder platformContextBuilder = TestPlatformContextBuilder.create();
        final TestConfigBuilder configBuilder = new TestConfigBuilder();
        if (configModifier != null) {
            configModifier.apply(configBuilder);
        }
        // add temp directory to config for marker files.
        configBuilder
                .withValue(
                        PathsConfig_.MARKER_FILES_DIR,
                        tempDir.resolve(TEST_MARKER_FILE_DIRECTORY).toString())
                .withValue(PathsConfig_.WRITE_PLATFORM_MARKER_FILES, true);
        // add configuration to platform builder.
        platformContextBuilder.withConfiguration(configBuilder.getOrCreateConfig());
        if (platformContextModifier != null) {
            // apply any other modifications to the platform builder.
            platformContextModifier.apply(platformContextBuilder);
        }
        return platformContextBuilder.build();
    }

    private @NonNull List<EventSource> generateSources(final int numNetworkNodes) {
        final List<EventSource> list = new LinkedList<>();
        for (long i = 0; i < numNetworkNodes; i++) {
            list.add(new StandardEventSource(true));
        }
        return list;
    }
}
