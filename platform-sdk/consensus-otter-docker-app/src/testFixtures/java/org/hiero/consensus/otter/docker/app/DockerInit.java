// SPDX-License-Identifier: Apache-2.0
package org.hiero.consensus.otter.docker.app;

import static com.swirlds.common.io.utility.FileUtils.getAbsolutePath;
import static com.swirlds.common.io.utility.FileUtils.rethrowIO;
import static com.swirlds.common.threading.manager.AdHocThreadManager.getStaticThreadManager;
import static com.swirlds.platform.builder.PlatformBuildConstants.DEFAULT_OVERRIDES_YAML_FILE_NAME;
import static com.swirlds.platform.builder.PlatformBuildConstants.DEFAULT_SETTINGS_FILE_NAME;
import static com.swirlds.platform.builder.internal.StaticPlatformBuilder.setupGlobalMetrics;
import static com.swirlds.platform.crypto.CryptoStatic.initNodeSecurity;
import static com.swirlds.platform.state.signed.StartupStateUtils.getInitialState;
import static com.swirlds.platform.system.address.AddressBookUtils.initializeAddressBook;

import com.swirlds.base.time.Time;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.io.filesystem.FileSystemManager;
import com.swirlds.common.io.utility.RecycleBin;
import com.swirlds.common.merkle.crypto.MerkleCryptography;
import com.swirlds.common.merkle.crypto.MerkleCryptographyFactory;
import com.swirlds.common.metrics.noop.NoOpMetrics;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.MerkleDb;
import com.swirlds.platform.SwirldsPlatform;
import com.swirlds.platform.builder.PlatformBuilder;
import com.swirlds.platform.state.ConsensusStateEventHandler;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.signed.HashedReservedSignedState;
import com.swirlds.platform.state.signed.ReservedSignedState;
import com.swirlds.platform.system.address.AddressBookUtils;
import com.swirlds.platform.test.fixtures.addressbook.RandomAddressBookBuilder;
import com.swirlds.platform.util.BootstrapUtils;
import com.swirlds.state.State;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.hiero.base.crypto.Cryptography;
import org.hiero.base.crypto.CryptographyProvider;
import org.hiero.consensus.model.node.KeysAndCerts;
import org.hiero.consensus.model.node.NodeId;
import org.hiero.consensus.model.roster.AddressBook;
import org.hiero.consensus.roster.RosterHistory;
import org.hiero.consensus.roster.RosterUtils;

public class DockerInit {


    public static void main(String[] args) throws Exception {
        final NodeId nodeId = NodeId.of(1L);
        final var appMain = new DockerApp();

        final ConfigurationBuilder configBuilder = ConfigurationBuilder.create();
        final List<Class<? extends Record>> configTypes = appMain.getConfigDataTypes();
        for (final Class<? extends Record> configType : configTypes) {
            configBuilder.withConfigDataType(configType);
        }

        rethrowIO(() -> BootstrapUtils.setupConfigBuilder(
                configBuilder,
                getAbsolutePath(DEFAULT_SETTINGS_FILE_NAME),
                getAbsolutePath(DEFAULT_OVERRIDES_YAML_FILE_NAME)));
        final Configuration configuration = configBuilder.build();

        setupGlobalMetrics(configuration);
        final var bootstrapAddressBook = new AddressBook(StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                RandomAddressBookBuilder.create(new Random())
                                        .withSize(1)
                                        .withRealKeysEnabled(true)
                                        .build()
                                        .iterator(),
                                0),
                        false)
                .map(address ->
                        address.copySetMemo("0.0." + (address.getNodeId().id() + 3)))
                .toList());
        final RecycleBin recycleBin = RecycleBin.create(
                new NoOpMetrics(),
                configuration,
                getStaticThreadManager(),
                Time.getCurrent(),
                FileSystemManager.create(configuration),
                nodeId);
        final Cryptography cryptography = CryptographyProvider.getInstance();
        final KeysAndCerts keysAndCerts = initNodeSecurity(
                bootstrapAddressBook, configuration, Set.of(nodeId))
                .get(nodeId);

        // the AddressBook is not changed after this point, so we calculate the hash now
        cryptography.digestSync(bootstrapAddressBook);

        // Set the MerkleCryptography instance for this node
        final MerkleCryptography merkleCryptography = MerkleCryptographyFactory.create(configuration);

        // Register with the ConstructableRegistry classes which need configuration.
        BootstrapUtils.setupConstructableRegistryWithConfiguration(configuration);

        // Create platform context
        final PlatformContext platformContext = PlatformContext.create(
                configuration,
                Time.getCurrent(),
                new NoOpMetrics(),
                FileSystemManager.create(configuration),
                recycleBin,
                merkleCryptography);
        // Each platform needs a different temporary state on disk.
        MerkleDb.resetDefaultInstancePath();
        PlatformStateFacade platformStateFacade = new PlatformStateFacade();
        // Create the initial state for the platform
        ConsensusStateEventHandler consensusStateEventHandler = appMain.newConsensusStateEvenHandler();
        final HashedReservedSignedState reservedState = getInitialState(
                recycleBin,
                appMain.getSemanticVersion(),
                appMain::newStateRoot,
                appMain.getClass().getName(),
                "org.hiero.consensus.otter.docker.app.DockerApp",
                nodeId,
                bootstrapAddressBook,
                platformStateFacade,
                platformContext);
        final ReservedSignedState initialState = reservedState.state();

        // Initialize the address book
        final AddressBook addressBook = initializeAddressBook(
                nodeId,
                appMain.getSemanticVersion(),
                initialState,
                bootstrapAddressBook,
                platformContext,
                consensusStateEventHandler,
                platformStateFacade);

        // Build the platform with the given values
        final State state = initialState.get().getState();
        final RosterHistory rosterHistory = RosterUtils.createRosterHistory(state);

        final PlatformBuilder builder = PlatformBuilder.create(
                appMain.getClass().getName(),
                "org.hiero.consensus.otter.docker.app.DockerApp",
                appMain.getSemanticVersion(),
                initialState,
                consensusStateEventHandler,
                nodeId,
                AddressBookUtils.formatConsensusEventStreamName(addressBook, nodeId),
                rosterHistory,
                platformStateFacade);
        builder.withSystemTransactionEncoderCallback(appMain::encodeSystemTransaction);

        // Build platform using the Inversion of Control pattern by injecting all needed
        // dependencies into the PlatformBuilder.
        final SwirldsPlatform platform = (SwirldsPlatform) builder.withConfiguration(configuration)
                .withPlatformContext(platformContext)
                .withKeysAndCerts(keysAndCerts)
                .build();
        platform.start();
    }
}
