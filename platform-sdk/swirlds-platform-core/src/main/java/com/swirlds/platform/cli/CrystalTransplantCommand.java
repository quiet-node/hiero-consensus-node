// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.cli;

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
import java.io.IOException;
import java.nio.file.Path;
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

    private void truncatePCESFilesIfNotAFreezeState() {
        try {
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
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void copyPCESFilesToCorrectDirectory() {}

    private void performConfigBump() {}
}
