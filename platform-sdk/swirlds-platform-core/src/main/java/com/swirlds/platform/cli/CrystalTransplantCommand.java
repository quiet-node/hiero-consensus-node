// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.cli;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.util.HapiUtils;
import com.swirlds.base.time.Time;
import com.swirlds.cli.utility.AbstractCommand;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.io.filesystem.FileSystemManager;
import com.swirlds.common.io.utility.SimpleRecycleBin;
import com.swirlds.common.merkle.crypto.MerkleCryptographyFactory;
import com.swirlds.common.metrics.noop.NoOpMetrics;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.platform.state.SavedStateUtils;
import com.swirlds.platform.state.snapshot.SavedStateMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import picocli.CommandLine;

@CommandLine.Command(
        name = "crystal-transplant",
        mixinStandardHelpOptions = true,
        description = "Cast a crystallization spell preparing a node to load a transplanted state")
public class CrystalTransplantCommand extends AbstractCommand {
    /** The return code for a successful operation. */
    private static final int RETURN_CODE_SUCCESS = 0;
    /** The return code when the user does not confirm the prompt */
    private static final int RETURN_CODE_PROMPT_NO = 1;

    /** Config Key for version */
    private static final String CONFIG_KEY = "hedera.config.version";
    /** Application properties file name */
    private static final String APPLICATION_PROPERTIES_FILE_NAME = "application.properties";

    /** The path to the state to prepare for transplant. */
    private Path statePath;

    @CommandLine.Option(
            names = {"-ac", "--auto-confirm"},
            description = "Automatically confirm the operation without prompting")
    @SuppressWarnings("unused") // used by picocli
    private boolean autoConfirm;

    private PlatformContext platformContext;

    /**
     * Set the path to state to prepare for transplant.
     */
    @CommandLine.Parameters(description = "The path to the state to load")
    @SuppressWarnings("unused") // used by picocli
    private void setStatePath(final Path statePath) {
        this.statePath = pathMustExist(statePath.toAbsolutePath());
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

    private void validateOverrideNetworkJson() {}

    private void printDiffsAndGetConfirmation() {}

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
}
