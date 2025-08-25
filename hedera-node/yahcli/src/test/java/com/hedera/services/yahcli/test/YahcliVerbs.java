// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.yahcli.test;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicReference;

public class YahcliVerbs {
    public static final AtomicReference<String> DEFAULT_CONFIG_LOC = new AtomicReference<>();
    public static final AtomicReference<String> DEFAULT_WORKING_DIR = new AtomicReference<>();
    public static final String TEST_NETWORK = "hapi";

    private YahcliVerbs() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Returns an operation that invokes a yahcli {@code accounts} subcommand with the given args,
     * taking the config location and working directory from defaults if not overridden.
     * @return the operation
     */
    public static YahcliCallOperation yahcliAccounts(@NonNull final String... args) {
        requireNonNull(args);
        return new YahcliCallOperation(prepend(args, "accounts"));
    }

    /**
     * Prepend the given strings to the front of the given array.
     * @param a the array
     * @param ps the strings to prepend
     * @return a new array with the strings prepended
     */
    public static String[] prepend(@NonNull final String[] a, @NonNull final String... ps) {
        requireNonNull(a);
        requireNonNull(ps);
        final var newArgs = new String[a.length + ps.length];
        System.arraycopy(ps, 0, newArgs, 0, ps.length);
        System.arraycopy(a, 0, newArgs, ps.length, a.length);
        return newArgs;
    }

    /**
     * Sets the default config location to use for yahcli operations.
     * @param configLoc the config location
     */
    public static void setDefaultConfigLoc(@NonNull final String configLoc) {
        requireNonNull(configLoc);
        DEFAULT_CONFIG_LOC.set(requireNonNull(configLoc));
    }

    /**
     * Sets the default working directory to use for yahcli operations.
     * @param workingDir the working directory
     */
    public static void setDefaultWorkingDir(@NonNull final String workingDir) {
        requireNonNull(workingDir);
        DEFAULT_WORKING_DIR.set(requireNonNull(workingDir));
    }
}
