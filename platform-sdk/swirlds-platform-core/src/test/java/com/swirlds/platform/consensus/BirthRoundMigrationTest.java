package com.swirlds.platform.consensus;

import com.swirlds.common.test.fixtures.WeightGenerator;
import com.swirlds.platform.test.fixtures.PlatformTest;
import com.swirlds.platform.test.fixtures.consensus.ConsensusTestParams;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class BirthRoundMigrationTest extends PlatformTest {

    @ParameterizedTest
    @MethodSource("com.swirlds.platform.consensus.ConsensusTestArgs#orderInvarianceTests")
    void test(final ConsensusTestParams params) {

    }

    private static void withSeed(
            final int numNodes,
            final @NonNull WeightGenerator weightGenerator,
            final long seed){

    }
}
