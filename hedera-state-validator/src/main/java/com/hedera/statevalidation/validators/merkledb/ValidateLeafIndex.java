/*
 * Copyright (C) 2023-2024 Hedera Hashgraph, LLC
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

import static com.hedera.statevalidation.validators.ParallelProcessingUtil.processRange;
import static com.hedera.statevalidation.validators.Utils.printFileDataLocationError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.hedera.statevalidation.merkledb.reflect.MemoryIndexDiskKeyValueStoreW;
import com.hedera.statevalidation.parameterresolver.ReportResolver;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceProvider;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceRecord;
import com.hedera.statevalidation.reporting.Report;
import com.hedera.statevalidation.reporting.SlackReportGenerator;
import com.swirlds.merkledb.MerkleDbDataSource;
import com.swirlds.merkledb.MerkleDbDataSourceW;
import com.swirlds.merkledb.files.DataFileCollectionW;
import com.swirlds.state.merkle.disk.OnDiskKey;
import com.swirlds.state.merkle.disk.OnDiskValue;
import com.swirlds.virtualmap.VirtualKey;
import com.swirlds.virtualmap.VirtualMap;
import com.swirlds.virtualmap.VirtualValue;
import com.swirlds.virtualmap.datasource.VirtualLeafBytes;

import com.swirlds.virtualmap.serialize.KeySerializer;
import com.swirlds.virtualmap.serialize.ValueSerializer;
import java.io.IOException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

// todo hackathon import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.ArgumentsSource;

@SuppressWarnings("NewClassNamingConvention")
@ExtendWith({ReportResolver.class, SlackReportGenerator.class})
@Tag("leaf")
@Log4j2
public class ValidateLeafIndex {

    @SuppressWarnings("unchecked")
    // todo hackathon @ParameterizedRepeatedIfExceptionsTest
    @ArgumentsSource(VirtualMapAndDataSourceProvider.class)
    public void validateIndex(VirtualMapAndDataSourceRecord<?, ?> dsRecord, Report report) {
        if(dsRecord.dataSource().getFirstLeafPath() == -1) {
            log.info("Skipping the validation for {} as the map is empty", dsRecord.name());
            return;
        }
        MerkleDbDataSourceW vds = dsRecord.createMerkleDSWrapper();
        MerkleDbDataSource merkleDb;
        final VirtualMap<VirtualKey, VirtualValue> map = (VirtualMap<VirtualKey, VirtualValue>) dsRecord.map();
        final KeySerializer keySerializer = dsRecord.keySerializer();
        final ValueSerializer valueSerializer = dsRecord.valueSerializer();

        merkleDb = dsRecord.dataSource();
        log.debug(vds.getPathToHashDisk().getFilesSizeStatistics());

        long firstLeafPath = merkleDb.getFirstLeafPath();
        long lastLeafPath = merkleDb.getLastLeafPath();

        var leafNodeIndex = vds.getLeafPathToDiskLocation();
        var objectKeyToPath = vds.getKeyToPath();
        var leafStore = new MemoryIndexDiskKeyValueStoreW<>(vds.getPathToKeyValue());
        var leafDfc = new DataFileCollectionW<>(leafStore.getFileCollection(), VirtualLeafBytes::parseFrom);

        assertEquals(lastLeafPath, leafNodeIndex.size() - 1);

        // iterate over internalNodeIndex and validate it
        ForkJoinTask<?> emptyIndexTask =
                processRange(0, firstLeafPath, path -> assertEquals(0, leafNodeIndex.get(path)));

        var nullErrorCount = new AtomicInteger(0);
        var exceptionCount = new AtomicInteger(0);
        var successCount = new AtomicInteger(0);

        LongConsumer indexProcessor = path -> {
            long dataLocation = leafNodeIndex.get(path, -1);
            assertNotEquals(-1, dataLocation);
            // read from dataLocation using datasource
            VirtualLeafBytes data;
            try {
                data = leafDfc.readDataItem(dataLocation);
                if (data != null) {
                    assertEquals(data.path(), path);
                    OnDiskKey key = (OnDiskKey) keySerializer.deserialize(data.keyBytes().toReadableSequentialData());
                    long actual = objectKeyToPath.get(data.keyBytes(), key.hashCode(), -1);
                    assertEquals(path, actual);

                    OnDiskValue value = (OnDiskValue) valueSerializer.deserialize(data.valueBytes().toReadableSequentialData());
                    assertEquals(value.getValue(), ((OnDiskValue) map.get(key)).getValue());
                    successCount.incrementAndGet();
                } else {
                    nullErrorCount.incrementAndGet();
                    printFileDataLocationError(log, "Missing entry on disk!", leafDfc, dataLocation);
                }
            } catch (IOException e) {
                exceptionCount.incrementAndGet();
                printFileDataLocationError(log, e.getMessage(), leafDfc, dataLocation);
            }
        };

        ForkJoinTask<?> nonEmptyIndexTask = processRange(firstLeafPath, lastLeafPath, indexProcessor);
        emptyIndexTask.join();
        nonEmptyIndexTask.join();

        log.debug("size of index: {}", leafNodeIndex.size());
        assertEquals(
                0,
                nullErrorCount.get(),
                "Some entries on disk are missing even though pointers are present in the index");
        assertEquals(0, exceptionCount.get(), "Some read operations failed");
        log.info("Successfully checked {} entries", successCount.get());
    }
}
