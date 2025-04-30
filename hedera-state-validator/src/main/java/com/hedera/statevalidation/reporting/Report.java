/*
 * Copyright (C) 2023 Hedera Hashgraph, LLC
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

package com.hedera.statevalidation.reporting;

import static com.hedera.statevalidation.validators.Constants.NODE_NAME;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

@Data
public class Report {
    private String nodeName = NODE_NAME;

    @InvariantProperty
    private long roundNumber;

    private long numberOfAccounts;
    private StateReport stateReport = new StateReport();
    private Map<String, VirtualMapReport> vmapReportByName = new HashMap<>();

}
