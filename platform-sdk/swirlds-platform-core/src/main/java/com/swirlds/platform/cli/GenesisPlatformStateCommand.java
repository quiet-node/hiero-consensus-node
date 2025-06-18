// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.cli;

import static com.swirlds.platform.state.service.PlatformStateFacade.DEFAULT_PLATFORM_STATE_FACADE;
import static com.swirlds.platform.state.snapshot.SavedStateMetadata.NO_NODE_ID;
import static com.swirlds.platform.state.snapshot.SignedStateFileWriter.writeSignedStateFilesToDirectory;

import com.swirlds.cli.commands.StateCommand;
import com.swirlds.cli.utility.AbstractCommand;
import com.swirlds.cli.utility.SubcommandOf;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.platform.config.DefaultConfiguration;
import com.swirlds.platform.consensus.SyntheticSnapshot;
import com.swirlds.platform.state.PlatformStateAccessor;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.signed.ReservedSignedState;
import com.swirlds.platform.state.snapshot.DeserializedSignedState;
import com.swirlds.platform.state.snapshot.SignedStateFileReader;
import com.swirlds.platform.util.BootstrapUtils;
import com.swirlds.state.BinaryState;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import org.hiero.consensus.roster.WritableRosterStore;
import picocli.CommandLine;

@CommandLine.Command(
        name = "genesis",
        mixinStandardHelpOptions = true,
        description = "Edit an existing state by replacing the platform state with a new genesis state.")
@SubcommandOf(StateCommand.class)
public class GenesisPlatformStateCommand extends AbstractCommand {
    private Path statePath;
    private Path outputDir;

    /**
     * The path to state to edit
     */
    @CommandLine.Parameters(description = "The path to the state to edit", index = "0")
    private void setStatePath(final Path statePath) {
        this.statePath = pathMustExist(statePath.toAbsolutePath());
    }

    /**
     * The path to the output directory
     */
    @CommandLine.Parameters(description = "The path to the output directory", index = "1")
    private void setOutputDir(final Path outputDir) {
        this.outputDir = dirMustExist(outputDir.toAbsolutePath());
    }

    @Override
    public Integer call() throws IOException, ExecutionException, InterruptedException {
        final Configuration configuration = DefaultConfiguration.buildBasicConfiguration(ConfigurationBuilder.create());
        BootstrapUtils.setupConstructableRegistry();

        final PlatformContext platformContext = PlatformContext.create(configuration);

        System.out.printf("Reading from %s %n", statePath.toAbsolutePath());
        final PlatformStateFacade stateFacade = DEFAULT_PLATFORM_STATE_FACADE;
        final DeserializedSignedState deserializedSignedState = SignedStateFileReader.readStateFile(
                statePath,
                (virtualMap) -> {
                    // FUTURE WORK: https://github.com/hiero-ledger/hiero-consensus-node/issues/19003
                    throw new UnsupportedOperationException();
                },
                stateFacade,
                platformContext);
        try (final ReservedSignedState reservedSignedState = deserializedSignedState.reservedSignedState()) {
            stateFacade.bulkUpdateOf(reservedSignedState.get().getState().getBinaryState(), v -> {
                System.out.printf("Replacing platform data %n");
                v.setRound(PlatformStateAccessor.GENESIS_ROUND);
                v.setSnapshot(SyntheticSnapshot.getGenesisSnapshot());
            });
            {
                System.out.printf("Resetting the RosterService state %n");
                final BinaryState state = reservedSignedState.get().getState().getBinaryState();
                final WritableRosterStore writableRosterStore = new WritableRosterStore(state);
                writableRosterStore.resetRosters();
            }
            System.out.printf("Hashing state %n");
            platformContext
                    .getMerkleCryptography()
                    .digestTreeAsync(reservedSignedState.get().getState().getRoot())
                    .get();
            System.out.printf("Writing modified state to %s %n", outputDir.toAbsolutePath());
            writeSignedStateFilesToDirectory(
                    platformContext, NO_NODE_ID, outputDir, reservedSignedState.get(), stateFacade);
        }

        return 0;
    }
}
