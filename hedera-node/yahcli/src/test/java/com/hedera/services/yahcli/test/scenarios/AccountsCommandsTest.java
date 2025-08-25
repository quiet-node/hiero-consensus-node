// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.yahcli.test.scenarios;

import static com.hedera.services.bdd.spec.HapiSpec.hapiTest;
import static com.hedera.services.bdd.spec.transactions.TxnVerbs.cryptoCreate;
import static com.hedera.services.yahcli.test.YahcliOperation.yahcliAccounts;
import static com.hedera.services.yahcli.test.YahcliTestBase.REGRESSION;

import com.hedera.services.bdd.junit.HapiTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag(REGRESSION)
public class AccountsCommandsTest {
    @HapiTest
    final Stream<DynamicTest> readmeAccountsCreateExample() {
        return hapiTest(
                cryptoCreate("somebody"),
                yahcliAccounts("create", "-d", "hbar", "-a", "1", "--memo", "Who danced between"));
    }
}
