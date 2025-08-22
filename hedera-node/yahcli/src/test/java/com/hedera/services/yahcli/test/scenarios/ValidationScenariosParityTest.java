package com.hedera.services.yahcli.test.scenarios;

import com.hedera.services.bdd.junit.HapiTest;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

import java.util.stream.Stream;

import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.cryptoCreate;
import static com.hedera.services.yahcli.test.YahcliTestBase.REGRESSION;

@Tag(REGRESSION)
public class ValidationScenariosParityTest {
    @HapiTest
    final Stream<DynamicTest> accountScenarioNoExtantEntities() {
        return hapiTest(
                cryptoCreate("somebody"));
    }
}
