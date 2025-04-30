/*
 * Copyright (C) 2022-2023 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hedera.statevalidation.validators;

import com.hedera.statevalidation.validator.ValidatorBase;
import lombok.extern.log4j.Log4j2;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Log4j2
@Command(name = "validate", mixinStandardHelpOptions = true, version = "0.39",
        description = "Validates the state of a Mainnet Hedera node")
public class Validator extends ValidatorBase {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        int exitCode = new CommandLine(new Validator()).execute(args);
        log.info("Execution time: {}ms", System.currentTimeMillis() - startTime);
        System.exit(exitCode);
    }

}