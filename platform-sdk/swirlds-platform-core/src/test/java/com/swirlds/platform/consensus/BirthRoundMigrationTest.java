package com.swirlds.platform.consensus;

import com.hedera.hapi.node.state.roster.Roster;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.test.fixtures.Randotron;
import com.swirlds.common.test.fixtures.WeightGenerator;
import com.swirlds.platform.test.fixtures.PlatformTest;
import com.swirlds.platform.test.fixtures.addressbook.RandomRosterBuilder;
import com.swirlds.platform.test.fixtures.consensus.ConsensusTestParams;
import com.swirlds.platform.test.fixtures.consensus.framework.ConsensusTestNode;
import com.swirlds.platform.test.fixtures.consensus.framework.ConsensusTestOrchestrator;
import com.swirlds.platform.test.fixtures.consensus.framework.OrchestratorBuilder;
import com.swirlds.platform.test.fixtures.consensus.framework.TestInput;
import com.swirlds.platform.test.fixtures.event.emitter.ShuffledEventEmitter;
import com.swirlds.platform.test.fixtures.event.generator.StandardGraphGenerator;
import com.swirlds.platform.test.fixtures.event.source.EventSource;
import com.swirlds.platform.test.fixtures.event.source.StandardEventSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.hiero.base.utility.test.fixtures.RandomUtils;
import org.hiero.consensus.config.EventConfig_;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class BirthRoundMigrationTest extends PlatformTest {
    private static final int NUMBER_OF_EVENTS = 10_000;

    @ParameterizedTest
    @MethodSource("com.swirlds.platform.consensus.ConsensusTestArgs#orderInvarianceTests")
    void test(final ConsensusTestParams params) {
        if (params.seeds().length == 0) {
            // run with random seed
            final long seed = new Random().nextLong();
            System.out.println("Running seed: " + seed);
            withSeed(params.numNodes(), params.weightGenerator(), seed);
        } else {
            // run with explicit seed
            for (final long seed : params.seeds()) {
                withSeed(params.numNodes(), params.weightGenerator(), seed);
            }
        }
    }

    private void withSeed(
            final int numNodes,
            final @NonNull WeightGenerator weightGenerator,
            final long seed) {
        final List<EventSource> eventSources = Stream.generate(StandardEventSource::new).limit(numNodes)
                .map(ses->(EventSource)ses)
                .toList();
        final Roster roster = RandomRosterBuilder.create(new Random(seed)).withWeightGenerator(weightGenerator)
                .withSize(numNodes).build();

        final PlatformContext preMigrationContext = createPlatformContext(
                null,
                configBuilder ->
                        configBuilder.withValue(EventConfig_.USE_BIRTH_ROUND_ANCIENT_THRESHOLD, false));
        final StandardGraphGenerator graphGenerator = new StandardGraphGenerator(
                preMigrationContext,
                seed,
                eventSources,
                roster
        );
        final ConsensusTestNode node1 = ConsensusTestNode.genesisContext(
                preMigrationContext,
                new ShuffledEventEmitter(graphGenerator, seed)
        );

    }
}
