// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.yahcli.test;

import static java.util.Objects.requireNonNull;

import com.hedera.services.bdd.junit.hedera.subprocess.SubProcessNetwork;
import com.hedera.services.bdd.spec.HapiSpec;
import com.hedera.services.bdd.spec.SpecOperation;
import com.hedera.services.yahcli.Yahcli;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import picocli.CommandLine;

/**
 * Executes a Yahcli command with the provided arguments against
 * the {@link SubProcessNetwork} targeted by the containing spec.
 */
public class YahcliOperation implements SpecOperation {
    private static final AtomicReference<String> DEFAULT_CONFIG_LOC = new AtomicReference<>();

    public static void setDefaultConfigLoc(@NonNull final String path) {
        requireNonNull(path);
        DEFAULT_CONFIG_LOC.set(requireNonNull(path));
    }

    private final String[] args;

    @Nullable
    private String configLoc;

    public YahcliOperation(@NonNull final String[] args) {
        this.args = requireNonNull(args);
    }

    public YahcliOperation withConfig(@NonNull final String configLoc) {
        this.configLoc = requireNonNull(configLoc);
        return this;
    }

    public static YahcliOperation yahcli(@NonNull final String... args) {
        requireNonNull(args);
        return new YahcliOperation(args);
    }

    public static YahcliOperation yahcliAccounts(@NonNull final String... args) {
        requireNonNull(args);
        final var newArgs = new String[args.length + 1];
        newArgs[0] = "accounts";
        System.arraycopy(args, 0, newArgs, 1, args.length);
        return new YahcliOperation(newArgs);
    }

    @Override
    public Optional<Throwable> execFor(@NonNull final HapiSpec spec) {
        requireNonNull(spec);
        final var commandLine = new CommandLine(new Yahcli());
        var finalizedArgs = args;
        if (!configProvidedViaArgs()) {
            final var c = Optional.ofNullable(configLoc)
                    .orElseGet(() -> requireNonNull(
                            DEFAULT_CONFIG_LOC.get(), "No default config location set, and none provided via args"));
            finalizedArgs = prepend(finalizedArgs, "-c", c);
        }
        final int rc = commandLine.execute(finalizedArgs);
        if (rc != 0) {
            return Optional.of(Assertions.fail("Yahcli command failed with exit code " + rc));
        }
        return Optional.empty();
    }

    private static String[] prepend(@NonNull final String[] a, @NonNull final String... ps) {
        requireNonNull(a);
        requireNonNull(ps);
        final var newArgs = new String[a.length + ps.length];
        System.arraycopy(ps, 0, newArgs, 0, ps.length);
        System.arraycopy(a, 0, newArgs, ps.length, a.length);
        return newArgs;
    }

    private boolean configProvidedViaArgs() {
        return argsInclude("-c") || argsInclude("--config");
    }

    private boolean argsInclude(String s) {
        for (final var arg : args) {
            if (arg.equals(s)) {
                return true;
            }
        }
        return false;
    }
}
