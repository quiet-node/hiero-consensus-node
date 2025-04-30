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

package com.hedera.statevalidation.parameterresolver;

import static com.hedera.statevalidation.parameterresolver.InitUtils.createTableConfigByNames;
import static com.hedera.statevalidation.parameterresolver.InitUtils.initConfiguration;
import static com.hedera.statevalidation.parameterresolver.InitUtils.initServiceRegistry;
import static com.hedera.statevalidation.parameterresolver.InitUtils.initVirtualMapRecords;

import com.hedera.node.app.services.ServicesRegistryImpl;
import com.swirlds.merkledb.MerkleDbTableConfig;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Log4j2
public class VirtualMapHolder {
    private static VirtualMapHolder instance;
    @Getter
    private final List<VirtualMapAndDataSourceRecord<?, ?>> records;
    @Getter
    private final Map<String, MerkleDbTableConfig> tableConfigByNames;

    private VirtualMapHolder() {
        initConfiguration();

        final ServicesRegistryImpl servicesRegistry = initServiceRegistry();
        tableConfigByNames = createTableConfigByNames();
        try {
            records = initVirtualMapRecords(servicesRegistry);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static VirtualMapHolder getInstance() {
        instance = (instance == null) ? new VirtualMapHolder() : instance;
        return instance;
    }

    public List<String> getTableNames() {
        return new ArrayList<>(tableConfigByNames.keySet());
    }

}
