// SPDX-License-Identifier: Apache-2.0
package org.hiero.consensus.otter.docker.app;

import static com.swirlds.logging.legacy.LogMarker.STARTUP;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.platform.event.StateSignatureTransaction;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.component.framework.model.DeterministicWiringModel;
import com.swirlds.platform.state.ConsensusStateEventHandler;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.system.Platform;
import com.swirlds.platform.system.SwirldMain;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.consensus.model.node.NodeId;

public class DockerApp implements SwirldMain<MerkleNodeState> {
    /** The logger for this class. */
    private static final Logger logger = LogManager.getLogger(DockerApp.class);

    private final SemanticVersion version = SemanticVersion.DEFAULT;

    /** The platform. */
    private Platform platform;

    private DeterministicWiringModel model;

    private ConsensusStateEventHandler<MerkleNodeState> consensusStateEventHandler;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(@NonNull final Platform platform, @NonNull final NodeId selfId) {
        Objects.requireNonNull(platform, "The platform must not be null.");
        Objects.requireNonNull(selfId, "The node id must not be null.");

        logger.info(STARTUP.getMarker(), "init called in Main for node {}.", selfId);
        this.platform = platform;
        //        consensusStateEventHandler = new SoloStateEventHandler();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {}

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public MerkleNodeState newStateRoot() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsensusStateEventHandler<MerkleNodeState> newConsensusStateEvenHandler() {
        return consensusStateEventHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public SemanticVersion getSemanticVersion() {
        return version;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public Bytes encodeSystemTransaction(@NonNull final StateSignatureTransaction transaction) {
        return Bytes.EMPTY;
    }

    private static void createFile(@NonNull final String name, @NonNull final String content) {
        final Path path = Paths.get(name + UUID.randomUUID() + ".txt");

        try {
            Files.createFile(path);
            Files.writeString(path, content);
            System.out.println("File created at: " + path.toAbsolutePath());
        } catch (FileAlreadyExistsException e) {
            System.out.println("File already exists.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final String content = String.join("\n", args);
        createFile("main.txt", content);
    }
}
