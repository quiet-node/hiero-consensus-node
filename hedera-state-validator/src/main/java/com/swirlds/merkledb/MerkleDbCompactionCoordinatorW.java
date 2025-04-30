/*
 * Copyright (C) 2024 Hedera Hashgraph, LLC
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

package com.swirlds.merkledb;

import com.hedera.statevalidation.validators.Constants;
import com.swirlds.virtualmap.VirtualKey;
import com.swirlds.virtualmap.VirtualValue;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MerkleDbCompactionCoordinatorW {
    private final MerkleDbDataSource dataSource;
    private final MerkleDbCompactionCoordinator coordinator;
    ConcurrentMap<String, Future<Boolean>> compactionFuturesByName;

    public MerkleDbCompactionCoordinatorW(MerkleDbDataSource dataSource) {
        try {
            this.dataSource = dataSource;
            coordinator = (MerkleDbCompactionCoordinator)
                    getField(MerkleDbDataSource.class, "compactionCoordinator").get(dataSource);
            compactionFuturesByName = (ConcurrentMap<String, Future<Boolean>>)
                    getField(MerkleDbCompactionCoordinator.class, "compactionFuturesByName")
                            .get(coordinator);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void runCompaction() {
        try {
            coordinator.compactDiskStoreForHashesAsync();
            coordinator.compactPathToKeyValueAsync();
            coordinator.compactDiskStoreForKeyToPathAsync();

            waitForCompactionToFinish();
            dataSource.snapshot(
                    Path.of(Constants.STATE_DIR, "tables", dataSource.getTableName() + "-" + dataSource.getTableId()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void waitForCompactionToFinish() throws InterruptedException, ExecutionException {
        for (Future<?> future : compactionFuturesByName.values()) {
            future.get();
        }
    }

    private Field getField(Class targetClass, String fieldName) {
        try {
            var field = targetClass.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
}
