package com.hedera.services.yahcli.test;

import com.hedera.services.bdd.junit.hedera.subprocess.SubProcessNetwork;
import com.hedera.services.bdd.spec.HapiSpec;
import com.hedera.services.bdd.spec.SpecOperation;
import com.hedera.services.yahcli.Yahcli;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import picocli.CommandLine;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Executes a Yahcli command with the provided arguments against
 * the {@link SubProcessNetwork} targeted by the containing spec.
 */
public class YahcliOperation implements SpecOperation {
    private final String[] args;

    public YahcliOperation(@NonNull final String[] args) {
        this.args = requireNonNull(args);
    }

    public static YahcliOperation yahcli(@NonNull final String... args) {
        return new YahcliOperation(args);
    }

    @Override
    public Optional<Throwable> execFor(@NonNull final HapiSpec spec) {
        final var commandLine = new CommandLine(new Yahcli());
        final int rc = commandLine.execute(args);
        if (rc != 0) {
            return Optional.of(Assertions.fail("Yahcli command failed with exit code " + rc));
        }
        return Optional.empty();
    }
}
