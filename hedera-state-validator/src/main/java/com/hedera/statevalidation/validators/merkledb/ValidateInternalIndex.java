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

package com.hedera.statevalidation.validators.merkledb;

import static com.hedera.statevalidation.validators.ParallelProcessingUtil.doNothing;
import static com.hedera.statevalidation.validators.ParallelProcessingUtil.processRange;
import static com.hedera.statevalidation.validators.Utils.printFileDataLocationError;
import static com.swirlds.merkledb.collections.LongList.IMPERMISSIBLE_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.statevalidation.merkledb.reflect.MemoryIndexDiskKeyValueStoreW;
import com.swirlds.merkledb.MerkleDbDataSourceW;
import com.hedera.statevalidation.parameterresolver.ReportResolver;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceProvider;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceRecord;
import com.hedera.statevalidation.reporting.Report;
import com.hedera.statevalidation.reporting.SlackReportGenerator;
import com.swirlds.merkledb.files.DataFileCollectionW;
import com.swirlds.virtualmap.VirtualKey;
import com.swirlds.virtualmap.VirtualValue;
import com.swirlds.virtualmap.datasource.VirtualHashRecord;
import com.swirlds.virtualmap.internal.cache.VirtualNodeCache;
// todo hackathon import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import lombok.extern.log4j.Log4j2;
import org.hiero.base.crypto.Hash;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/**
 * This tests validates the index for internal nodes of a virtual map.
 * It verifies that all the index pointers are pointing to valid data entries containing hashes.
 */
@SuppressWarnings("NewClassNamingConvention")
@ExtendWith({ReportResolver.class, SlackReportGenerator.class})
@Tag("internal")
@Log4j2
public class ValidateInternalIndex {
    // todo hackathon @ParameterizedRepeatedIfExceptionsTest
    @ArgumentsSource(VirtualMapAndDataSourceProvider.class)
    public void validateIndex(VirtualMapAndDataSourceRecord<VirtualKey, VirtualValue> record, Report report) {
        var dataSourceW = new MerkleDbDataSourceW(record.dataSource());
        if(record.dataSource().getFirstLeafPath() == -1) {
            log.info("Skipping the validation for {} as the map is empty", record.name());
            return;
        }

        final long inMemoryHashThreshold;
        var lastLeafPath = record.dataSource().getLastLeafPath();
        var internalNodesIndex = dataSourceW.getHashPathToDiskLocation();
        var internalStore = new MemoryIndexDiskKeyValueStoreW<>(dataSourceW.getPathToHashDisk());
        var dfc = new DataFileCollectionW<>(internalStore.getFileCollection(), VirtualHashRecord::parseFrom);
        var pathToHashRam = dataSourceW.getPathToHashRam();
        var inMemoryExceptionCount = new AtomicInteger();

        final ForkJoinTask<?> inMemoryTask;
        if (pathToHashRam != null) {
            inMemoryHashThreshold = dataSourceW.getHashesRamToDiskThreshold();
            assertTrue(
                    pathToHashRam.size() <= inMemoryHashThreshold,
                    "The size of the pathToHashRam should be less than or equal to the in memory hash threshold");
            var rightBoundary = Math.min(inMemoryHashThreshold, lastLeafPath);
            if (inMemoryHashThreshold >= lastLeafPath) {
                assertEquals(0, internalNodesIndex.size(), "The size of the index should be 0");
                log.info(
                        "Skipping test for {} as the in memory hash threshold is greater than the last leaf path, so the index is not used",
                        record.name());
                return;
            }
            LongConsumer inMemoryIndexProcessor = path -> {
                try {
                    Hash actual = pathToHashRam.get(path);
                    assertNotNull(actual, "The pathToHashRam should not be null");
                    assertNotEquals(actual, VirtualNodeCache.NULL_HASH, "The hash cannot be null hash");
                    assertEquals(IMPERMISSIBLE_VALUE, internalNodesIndex.get(path));
                } catch (IOException e) {
                    inMemoryExceptionCount.incrementAndGet();
                }
            };
            inMemoryTask = processRange(0, rightBoundary, inMemoryIndexProcessor);
            assertEquals(
                    lastLeafPath,
                    internalNodesIndex.size() - 1,
                    "The size of the index should be equal to the last leaf path");
        } else {
            inMemoryHashThreshold = 0;
            inMemoryTask = doNothing();
            assertEquals(
                    lastLeafPath + 1,
                    internalNodesIndex.size(),
                    "The size of the index should be equal to the difference between the last leaf path and the first leaf path in the index");
        }

        var nullErrorCount = new AtomicInteger(0);
        var onDiskExceptionCount = new AtomicInteger(0);
        var successCount = new AtomicInteger(0);

        // iterate over internalNodeIndex and validate it
        LongConsumer indexProcessor = path -> {
            long dataLocation = internalNodesIndex.get(path, -1);
            // read from dataLocation using datasource
            assertNotEquals(-1, dataLocation);
            try {
                var data = dfc.readDataItem(dataLocation);
                if (data == null) {
                    printFileDataLocationError(log, "Missing entry on disk!", dfc, dataLocation);
                    nullErrorCount.incrementAndGet();
                } else {
                    assertEquals(data.path(), path);
                    assertNotNull(data.hash());
                    successCount.incrementAndGet();
                }
            } catch (Exception e) {
                printFileDataLocationError(log, e.getMessage(), dfc, dataLocation);
                onDiskExceptionCount.incrementAndGet();
            }
        };

        log.debug("Size of index: " + internalNodesIndex.size());

        ForkJoinTask<?> onDiskTask = processRange(inMemoryHashThreshold, lastLeafPath, indexProcessor);
        inMemoryTask.join();
        onDiskTask.join();

        assertEquals(
                0,
                nullErrorCount.get(),
                "Some entries on disk are missing even though pointers are present in the index");
        assertEquals(0, inMemoryExceptionCount.get(), "Some read from memory operations failed");
        assertEquals(0, onDiskExceptionCount.get(), "Some read from disk operations failed");
        log.debug("Successfully checked {} entries", successCount.get());
        // FUTURE WORK: record these in the reporting data structure
        // https://github.com/hashgraph/hedera-services/issues/7229
    }
}
