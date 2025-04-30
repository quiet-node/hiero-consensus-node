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

import static com.hedera.statevalidation.validators.Constants.COLLECTED_INFO_THRESHOLD;
import static com.hedera.statevalidation.validators.Constants.VALIDATE_STALE_KEYS_EXCLUSIONS;
import static com.hedera.statevalidation.validators.ParallelProcessingUtil.processRange;
import static com.hedera.statevalidation.validators.Utils.printFileDataLocationError;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.statevalidation.merkledb.reflect.BucketIterator;
import com.hedera.statevalidation.merkledb.reflect.HalfDiskHashMapW;
import com.hedera.statevalidation.merkledb.reflect.MemoryIndexDiskKeyValueStoreW;
import com.swirlds.merkledb.MerkleDbDataSourceW;
import com.hedera.statevalidation.parameterresolver.ReportResolver;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceProvider;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceRecord;
import com.hedera.statevalidation.reporting.Report;
import com.hedera.statevalidation.reporting.SlackReportGenerator;
import com.swirlds.merkledb.files.DataFileCollectionW;
import com.swirlds.merkledb.files.hashmap.ParsedBucket;
import com.swirlds.virtualmap.VirtualKey;
import com.swirlds.virtualmap.VirtualMapW;
import com.swirlds.virtualmap.VirtualValue;
import com.swirlds.virtualmap.datasource.VirtualLeafBytes;
import com.swirlds.virtualmap.datasource.VirtualLeafRecord;
import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.LongConsumer;

@SuppressWarnings("NewClassNamingConvention")
@ExtendWith({ReportResolver.class, SlackReportGenerator.class})
@Tag("hdhm")
@Log4j2
public class ValidateLeafIndexHalfDiskHashMap {

    @ParameterizedRepeatedIfExceptionsTest
    @ArgumentsSource(VirtualMapAndDataSourceProvider.class)
    public void validateIndex(VirtualMapAndDataSourceRecord<VirtualKey, VirtualValue> vmAndSource, Report report) {
        if(vmAndSource.dataSource().getFirstLeafPath() == -1) {
            log.info("Skipping the validation for {} as the map is empty", vmAndSource.name());
            return;
        }

        VirtualMapW<VirtualKey, VirtualValue> map = vmAndSource.map();
        boolean skipStaleKeysValidation = VALIDATE_STALE_KEYS_EXCLUSIONS.contains(vmAndSource.name());
        MerkleDbDataSourceW vds = new MerkleDbDataSourceW(vmAndSource.dataSource());

        log.debug(vds.getPathToHashDisk().getFilesSizeStatistics());

        final var hdhm = new HalfDiskHashMapW(vds.getKeyToPath());
        final var leafStore = new MemoryIndexDiskKeyValueStoreW<>(vds.getPathToKeyValue());
        final var pathToDiskLocationLeafNodes = vds.getLeafPathToDiskLocation();
        Function<ReadableSequentialData, ParsedBucket> bucketDeser = v -> {
            ParsedBucket bucket = new ParsedBucket();
            bucket.readFrom(v);
            return bucket;
        };
        Function<ReadableSequentialData, VirtualLeafBytes> leafDeser = VirtualLeafBytes::parseFrom;
        final var dfc = new DataFileCollectionW(hdhm.getFileCollection(), bucketDeser);
        final var leafStoreDFC = new DataFileCollectionW(leafStore.getFileCollection(), leafDeser);
        final var stalePathsInfos = new CopyOnWriteArrayList<StalePathInfo>();
        final var nullLeafsInfo = new CopyOnWriteArrayList<NullLeafInfo>();
        final var unexpectedKeyInfos = new CopyOnWriteArrayList<UnexpectedKeyInfo>();
        final var pathMismatchInfos = new CopyOnWriteArrayList<PathMismatchInfo>();
        final var incorrectBucketIndexList = new CopyOnWriteArrayList<Integer>();
        LongConsumer consumer = i -> {
            long bucketLocation = 0;
            try {
                bucketLocation = hdhm.getBucketIndexToBucketLocation().get(i);
                if (bucketLocation == 0) {
                    return;
                }
                ParsedBucket bucket = (ParsedBucket) dfc.readDataItem(bucketLocation);
                if(bucket.getBucketIndex() != i) {
                    incorrectBucketIndexList.add(bucket.getBucketIndex());
                }
                var bucketIterator = new BucketIterator(bucket);
                long path;
                while (bucketIterator.hasNext()) {
                    ParsedBucket.BucketEntry entry = bucketIterator.next();
                    var key = map.getKeySerializer().fromBytes(entry.getKeyBytes());
                    path = entry.getValue();
                    // get path -> dataLocation
                    var dataLocation = pathToDiskLocationLeafNodes.get(path);
                    if (dataLocation == 0) {
                        collectInfo(new StalePathInfo(path, key), stalePathsInfos);
                        continue;
                    }
                    VirtualLeafRecord<?, ?> leaf = ((VirtualLeafBytes) leafStoreDFC.readDataItem(dataLocation)).toRecord(map.getKeySerializer(), map.getValueSerializer());
                    if (leaf == null) {
                        printFileDataLocationError(log, "Record with null leafs!", dfc, bucketLocation);
                        collectInfo(new NullLeafInfo(path, key), nullLeafsInfo);
                        continue;
                    }
                    if (!key.equals(leaf.getKey())) {
                        printFileDataLocationError(log, "Record with unexpected key!", dfc, bucketLocation);
                        collectInfo(new UnexpectedKeyInfo(path, key, leaf.getKey()), unexpectedKeyInfos);
                    }
                    if (leaf.getPath() != path) {
                        printFileDataLocationError(log, "Record with unexpected path!", dfc, bucketLocation);
                        collectInfo(new PathMismatchInfo(path, leaf.getPath(), key), pathMismatchInfos);
                    }
                }
            } catch (Exception e) {
                if (bucketLocation != 0) {
                   printFileDataLocationError(log, e.getMessage(), dfc, bucketLocation);
                }
                throw e;
            }
        };
        // iterate over all the buckets
        processRange(0, hdhm.getBucketIndexToBucketLocation().size(), consumer).join();
        if (!stalePathsInfos.isEmpty()) {
            log.error("Stale path info:\n{}", stalePathsInfos);
            log.error("There are {} records with stale paths, please check the logs for more info", stalePathsInfos.size());
        }

        if (!nullLeafsInfo.isEmpty()) {
            log.error("Null leaf info:\n{}", stalePathsInfos);
            log.error("There are {} records with null leafs, please check the logs for more info", stalePathsInfos.size());
        }

        if (!unexpectedKeyInfos.isEmpty()) {
            log.error("Unexpected key info:\n{}", unexpectedKeyInfos);
            log.error("There are {} records with unexpected keys, please check the logs for more info", unexpectedKeyInfos.size());
        }

        if (!pathMismatchInfos.isEmpty()) {
            log.error("Path mismatch info:\n{}", pathMismatchInfos);
            log.error("There are {} records with mismatched paths, please check the logs for more info", pathMismatchInfos.size());
        }

        assertTrue(
                (stalePathsInfos.isEmpty() || skipStaleKeysValidation) &&
                nullLeafsInfo.isEmpty() &&
                unexpectedKeyInfos.isEmpty() &&
                pathMismatchInfos.isEmpty() &&
                incorrectBucketIndexList.isEmpty(),
                "One of the test condition hasn't been met. " +
                        "Conditions: " +
                        "(stalePathsInfos.isEmpty() || skipStaleKeysValidation) = %s, nullLeafsInfo.isEmpty() = %s,  unexpectedKeyInfos.isEmpty() = %s, pathMismatchInfos.isEmpty() = %s, incorrectBucketIndexList.isEmpty() = %s. IncorrectBucketIndexInfos: %s"
                                .formatted(
                                (stalePathsInfos.isEmpty() || skipStaleKeysValidation),
                                nullLeafsInfo.isEmpty() ,
                                unexpectedKeyInfos.isEmpty() ,
                                pathMismatchInfos.isEmpty() ,
                                incorrectBucketIndexList.isEmpty(), incorrectBucketIndexList));
    }

    private static <T> void collectInfo(T info, CopyOnWriteArrayList<T> list) {
        if (COLLECTED_INFO_THRESHOLD == 0 || list.size() < COLLECTED_INFO_THRESHOLD) {
            list.add(info);
        }
    }

    record StalePathInfo(long path, VirtualKey key) {
        @Override
        public String toString() {
            return "StalePathInfo{" +
                    "path=" + path +
                    ", key=" + key +
                    "}\n";
        }
    }

    private record NullLeafInfo(long path, VirtualKey key) {
        @Override
        public String toString() {
            return "NullLeafInfo{" +
                    "path=" + path +
                    ", key=" + key +
                    "}\n";
        }
    }

    record UnexpectedKeyInfo(long path, VirtualKey expectedKey, VirtualKey actualKey) {
        @Override
        public String toString() {
            return "UnexpectedKeyInfo{" +
                    "path=" + path +
                    ", expectedKey=" + expectedKey +
                    ", actualKey=" + actualKey +
                    "}\n";
        }
    }

    private record PathMismatchInfo(long expectedPath, long actualPath, VirtualKey key) {
        @Override
        public String toString() {
            return "PathMismatchInfo{" +
                    "expectedPath=" + expectedPath +
                    ", actualPath=" + actualPath +
                    ", key=" + key +
                    "}\n";
        }
    }

}
