// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.cli;

import static com.swirlds.platform.util.BootstrapUtils.setupConstructableRegistry;
import static com.swirlds.platform.util.BootstrapUtils.setupConstructableRegistryWithConfiguration;
import static com.swirlds.virtualmap.constructable.ConstructableUtils.registerVirtualMapConstructables;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.state.roster.Roster;
import com.hedera.hapi.util.HapiUtils;
import com.hedera.node.internal.network.Network;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.swirlds.base.time.Time;
import com.swirlds.cli.utility.AbstractCommand;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.io.filesystem.FileSystemManager;
import com.swirlds.common.io.utility.SimpleRecycleBin;
import com.swirlds.common.merkle.crypto.MerkleCryptographyFactory;
import com.swirlds.common.metrics.noop.NoOpMetrics;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.platform.cli.utils.HederaAppUtils;
import com.swirlds.platform.state.SavedStateUtils;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.signed.StartupStateUtils;
import com.swirlds.platform.state.snapshot.SavedStateMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Console;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hiero.base.constructable.ConstructableRegistryException;
import org.hiero.consensus.model.node.NodeId;
import org.hiero.consensus.roster.RosterComparator;
import org.hiero.consensus.roster.RosterUtils;
import picocli.CommandLine;

@CommandLine.Command(
        name = "crystal-transplant",
        mixinStandardHelpOptions = true,
        description = "Cast a crystallization spell preparing a node to load a transplanted state")
public class CrystalTransplantCommand extends AbstractCommand {
    public static final String MAIN_CLASS_NAME = "com.hedera.services.ServicesMain";
    /** The return code for a successful operation. */
    private static final int RETURN_CODE_SUCCESS = 0;
    /** The return code when the user does not confirm the prompt */
    private static final int RETURN_CODE_PROMPT_NO = 1;

    /** Config Key for version */
    private static final String CONFIG_KEY = "hedera.config.version";
    /** Application properties file name */
    private static final String APPLICATION_PROPERTIES_FILE_NAME = "application.properties";

    public static final String SWIRLD_NAME = "123";

    /** The path to the state to prepare for transplant. */
    private Path statePath;

    private NodeId selfId;
    private Path networkOverrideFile;

    @CommandLine.Option(
            names = {"-ac", "--auto-confirm"},
            description = "Automatically confirm the operation without prompting")
    @SuppressWarnings("unused") // used by picocli
    private boolean autoConfirm;

    private PlatformContext platformContext;
    private Roster overrideRoster;

    /**
     * Set the path to state to prepare for transplant.
     */
    @CommandLine.Parameters(description = "The path to the state to load")
    @SuppressWarnings("unused") // used by picocli
    private void setStatePath(final Path statePath) {
        this.statePath = pathMustExist(statePath.toAbsolutePath());
    }

    @CommandLine.Parameters(description = "The path to the network override file")
    @SuppressWarnings("unused") // used by picocli
    private void setNetworkOverrideFile(final Path networkOverrideFile) {
        this.networkOverrideFile = pathMustExist(networkOverrideFile.toAbsolutePath());
    }

    @CommandLine.Option(
            names = {"-i", "--id"},
            required = true,
            description = "The ID of the target node")
    @SuppressWarnings("unused") // used by picocli
    private void setSelfId(final long selfId) {
        this.selfId = NodeId.of(selfId);
    }

    /**
     * This method is called after command line input is parsed.
     *
     * @return return code of the program
     */
    @Override
    public Integer call() throws IOException {
        final Configuration configuration =
                ConfigurationBuilder.create().autoDiscoverExtensions().build();

        platformContext = PlatformContext.create(
                configuration,
                Time.getCurrent(),
                new NoOpMetrics(),
                FileSystemManager.create(configuration),
                new SimpleRecycleBin(),
                MerkleCryptographyFactory.create(configuration));

        validateOverrideNetworkJson();

        printDiffsAndGetConfirmation();

        truncatePCESFilesIfNotAFreezeState();

        copyPCESFilesToCorrectDirectory();

        performConfigBump();

        return RETURN_CODE_SUCCESS;
    }

    private void validateOverrideNetworkJson() {
        final Roster roster = loadRosterFrom(networkOverrideFile);
        if (roster == null) {
            System.out.printf("Unable to load network override file %s%n", networkOverrideFile);
        }
        this.overrideRoster = roster;
    }

    private void printDiffsAndGetConfirmation() {
        final var versionString = platformContext.getConfiguration().getValue(CONFIG_KEY);
        final var version = Optional.ofNullable(versionString)
                .map(HapiUtils::fromString)
                .orElse(SemanticVersion.newBuilder().major(1).build());

        final Roster stateRoster = getStateRoster(version);
        System.out.println(RosterComparator.compare(stateRoster, this.overrideRoster));
        if (!this.autoConfirm) {
            Console console = System.console();
            String keepGoing = "";
            while (Objects.equals(keepGoing, "")) {
                if (console != null) {
                    keepGoing = console.readLine("Continue? [Y/N]:");
                } else {
                    // fallback for IDEs where System.console() is null
                    System.out.print("Continue? [Y/N]:");
                    keepGoing = new Scanner(System.in).nextLine();
                }
            }
            if (!keepGoing.equalsIgnoreCase("Y")) {
                System.exit(1);
            }
        }
    }

    private Roster getStateRoster(final SemanticVersion version) {
        setupConstructableRegistry();
        try {
            setupConstructableRegistryWithConfiguration(platformContext.getConfiguration());
            registerVirtualMapConstructables(platformContext.getConfiguration());
        } catch (ConstructableRegistryException e) {
            throw new RuntimeException(e);
        }

        try (final var state = StartupStateUtils.loadStateFile(
                new SimpleRecycleBin(),
                NodeId.FIRST_NODE_ID,
                MAIN_CLASS_NAME,
                SWIRLD_NAME,
                HederaAppUtils::createrNewMerkleNodeState,
                version,
                new PlatformStateFacade(),
                platformContext)) {
            final var rosterHistory =
                    RosterUtils.createRosterHistory(state.get().getState());
            return rosterHistory.getCurrentRoster();
        }
    }

    /**
     * If the state is not a freeze state, this method truncates the PCES files by removing events with future birth
     * rounds to make the state look like a freeze state
     */
    private void truncatePCESFilesIfNotAFreezeState() throws IOException {
        final SavedStateMetadata stateMetadata =
                SavedStateMetadata.parse(statePath.resolve(SavedStateMetadata.FILE_NAME));
        if (stateMetadata.freezeState() == null || !stateMetadata.freezeState()) {
            final int discardedEventCount = SavedStateUtils.prepareStateForTransplant(statePath, platformContext);
            System.out.printf(
                    "PCES file truncation complete. %d events were discarded due to being from a future round.%n",
                    discardedEventCount);
        } else {
            System.out.printf("The state is a freeze state, no truncation is needed.%n");
        }
    }

    private void copyPCESFilesToCorrectDirectory() {}

    /**
     * Updates the application properties file by increasing the patch version of the configuration key's semantic
     * version, if it exists. If the key is not found, it adds a new configuration entry with a default version. <br/>
     * The method performs the following steps: 1. Locates and reads the properties file. 2. Loops through each line to
     * find the version configuration key. If the key exists and is not commented out, it identifies the version,
     * increments the patch number, and updates that line. 3. If the configuration key is missing from the file, it adds
     * a new entry with a predefined version. 4. Saves the updated properties file.
     *
     * @throws IOException If the application properties file is missing or cannot be read or written.
     */
    private void performConfigBump() throws IOException {
        final Path propertiesPath = Paths.get(APPLICATION_PROPERTIES_FILE_NAME);
        if (Files.notExists(propertiesPath)) {
            throw new IOException("application.properties not found in current working directory: "
                    + propertiesPath.toAbsolutePath());
        }

        final List<String> lines = Files.readAllLines(propertiesPath, StandardCharsets.UTF_8);

        boolean updated = false;
        for (int i = 0; i < lines.size(); i++) {
            final String line = lines.get(i);
            if (isComment(line)) {
                continue;
            }

            final int idx = indexOfKey(line);
            if (idx >= 0) {
                final String originalValue = line.substring(idx).trim();
                final SemanticVersion parsed = HapiUtils.fromString(originalValue);
                final SemanticVersion bumped =
                        parsed.copyBuilder().patch(parsed.patch() + 1).build();
                final String bumpedValue = HapiUtils.toString(bumped);
                final String newLine = line.substring(0, idx) + bumpedValue;
                lines.set(i, newLine);
                updated = true;
                break; // Update only the first occurrence
            }
        }

        if (!updated) {
            final SemanticVersion newVersion =
                    SemanticVersion.newBuilder().major(1).build();
            lines.add(String.format("%s=%s", CONFIG_KEY, HapiUtils.toString(newVersion)));
        }

        Files.write(propertiesPath, lines, StandardCharsets.UTF_8);
    }

    /**
     * Checks whether a given line of text is a comment. A line is considered a comment if it starts with either '#' or
     * '!', ignoring leading whitespace during this check.
     *
     * @param line the line of text to check, must not be null
     * @return {@code true} if the line is a comment, {@code false} otherwise
     */
    private static boolean isComment(@NonNull final String line) {
        final String trimmed = line.stripLeading();
        return trimmed.startsWith("#") || trimmed.startsWith("!");
    }

    /**
     * Finds the index of the end of a key in a line of text. The key matches a predefined configuration key, which can
     * be optionally surrounded by whitespace and followed by either a colon (:) or an equal sign (=).
     *
     * @param line the line of text to search, must not be null
     * @return the index of the character immediately after the key if it is found, or -1 if the key is not found
     */
    private static int indexOfKey(@NonNull final String line) {
        Objects.requireNonNull(line, "line must not be null");
        final Pattern pattern = Pattern.compile("^\\s*" + Pattern.quote(CONFIG_KEY) + "\\s*[:=]");
        final Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            return matcher.end();
        }
        return -1;
    }

    /**
     * Attempts to load a {@link Roster} from a given network override valid file.
     *
     * @param path the path to the file to load the roster from
     * @return the loaded roster, if it was found and successfully loaded
     */
    public static Roster loadRosterFrom(@NonNull final Path path) {
        if (Files.exists(path)) {
            try (final var fin = Files.newInputStream(path)) {
                final var network = Network.JSON.parse(new ReadableStreamingData(fin));
                return RosterUtils.rosterFrom(network);
            } catch (Exception e) {
                System.err.printf("Failed to load %s network info from %s%n", path.toAbsolutePath(), e);
            }
        }
        return null;
    }
}
