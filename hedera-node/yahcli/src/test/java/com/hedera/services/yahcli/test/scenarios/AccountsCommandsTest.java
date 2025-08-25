// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.yahcli.test.scenarios;

import static com.hedera.services.bdd.spec.HapiPropertySource.asAccountString;
import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.assertions.AccountInfoAsserts.accountWith;
import static com.hedera.services.bdd.spec.queries.QueryVerbs.getAccountInfo;
import static com.hedera.services.bdd.spec.utilops.UtilVerbs.sourcingContextual;
import static com.hedera.services.bdd.suites.HapiSuite.ONE_HBAR;
import static com.hedera.services.yahcli.test.YahcliTestBase.REGRESSION;
import static com.hedera.services.yahcli.test.YahcliVerbs.yahcliAccounts;

import com.hedera.services.bdd.junit.HapiTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag(REGRESSION)
public class AccountsCommandsTest {
    @HapiTest
    final Stream<DynamicTest> readmeAccountsCreateExample() {
        return hapiTest(
                yahcliAccounts("create", "-d", "hbar", "-a", "1", "--memo", "Who danced between").exposingOutputTo(s -> {
                    System.out.println("-----");
                    System.out.println(s);
                    System.out.println("-----");
                }),
                sourcingContextual(spec -> getAccountInfo(
                                asAccountString(spec.accountIdFactory().apply(1001L)))
                        .has(accountWith().balance(ONE_HBAR).memo("Who danced between"))));
    }
}
