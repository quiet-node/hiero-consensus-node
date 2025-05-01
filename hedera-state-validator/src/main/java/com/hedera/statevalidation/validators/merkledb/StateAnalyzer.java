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

import static com.hedera.statevalidation.parameterresolver.InitUtils.CONFIGURATION;
import static com.hedera.statevalidation.validators.ParallelProcessingUtil.processObjects;
import static com.swirlds.base.units.UnitConstants.BYTES_TO_MEBIBYTES;
import static java.math.RoundingMode.HALF_UP;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.statevalidation.merkledb.reflect.MemoryIndexDiskKeyValueStoreW;
import com.swirlds.merkledb.MerkleDbDataSourceW;
import com.hedera.statevalidation.parameterresolver.ReportResolver;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceProvider;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceRecord;
import com.hedera.statevalidation.reporting.Report;
import com.hedera.statevalidation.reporting.SlackReportGenerator;
import com.hedera.statevalidation.reporting.StorageReport;
import com.hedera.statevalidation.reporting.VirtualMapReport;
import com.swirlds.merkledb.KeyRange;
import com.swirlds.merkledb.collections.LongList;
import com.swirlds.merkledb.collections.LongListHeap;
import com.swirlds.merkledb.files.DataFileCollection;
import com.swirlds.merkledb.files.DataFileIterator;
import com.swirlds.merkledb.files.DataFileReader;
import com.swirlds.virtualmap.VirtualMapW;
import com.swirlds.virtualmap.datasource.VirtualHashRecord;
import com.swirlds.virtualmap.datasource.VirtualLeafBytes;
import com.swirlds.virtualmap.datasource.VirtualLeafRecord;
// todo hackathon import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import lombok.extern.log4j.Log4j2;
import org.hiero.base.io.streams.SerializableDataOutputStream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

@ExtendWith({ReportResolver.class, SlackReportGenerator.class})
@Tag("stateAnalyzer")
@Log4j2
public class StateAnalyzer {

    // todo hackathon @ParameterizedRepeatedIfExceptionsTest
    @ArgumentsSource(VirtualMapAndDataSourceProvider.class)
    public void calculateDuplicatesForPathToKeyValueStorage(VirtualMapAndDataSourceRecord labelAndDs, Report report) {
        MerkleDbDataSourceW vds = labelAndDs.createMerkleDSWrapper();
        VirtualMapW map = labelAndDs.map();
        updateReport(
                labelAndDs,
                report,
                new MemoryIndexDiskKeyValueStoreW<>(vds.getPathToKeyValue()).getFileCollection(),
                VirtualMapReport::setPathToKeyValueReport,
                v -> {
                    VirtualLeafBytes virtualLeafBytes = VirtualLeafBytes.parseFrom(v);
                    return virtualLeafBytes.toRecord(map.getKeySerializer(), map.getValueSerializer());
                });
    }

    // todo hackathon @ParameterizedRepeatedIfExceptionsTest
    @ArgumentsSource(VirtualMapAndDataSourceProvider.class)
    public void calculateDuplicatesForPathToHashStorage(VirtualMapAndDataSourceRecord labelAndDs, Report report) {
        MerkleDbDataSourceW vds = labelAndDs.createMerkleDSWrapper();
        updateReport(
                labelAndDs,
                report,
                new MemoryIndexDiskKeyValueStoreW<>(vds.getPathToHashDisk()).getFileCollection(),
                VirtualMapReport::setPathToHashReport,
                VirtualHashRecord::parseFrom);
    }

    // todo hackathon @ParameterizedRepeatedIfExceptionsTest
    @ArgumentsSource(VirtualMapAndDataSourceProvider.class)
    public void calculateDuplicatesForObjectKeyToPathStorage(VirtualMapAndDataSourceRecord labelAndDs, Report report) {
//          MerkleDbDataSourceW vds = labelAndDs.createMerkleDSWrapper();
//          updateReport(labelAndDs, report, new HalfDiskHashMapW(vds.getKeyToPath()).getFileCollection(), VirtualMapReport::setObjectKeyToPathReport);
    }

    private void updateReport(
            VirtualMapAndDataSourceRecord labelAndDs,
            Report report,
            DataFileCollection dataFileCollection,
            BiConsumer<VirtualMapReport, StorageReport> vmReportUpdater,
            Function<ReadableSequentialData, ?> deser) {
        VirtualMapReport vmReport =
                report.getVmapReportByName().computeIfAbsent(labelAndDs.name(), k -> new VirtualMapReport());
        StorageReport storageReport = createStoreReport(dataFileCollection, deser);
        KeyRange validKeyRange = dataFileCollection.getValidKeyRange();
        storageReport.setMinPath(validKeyRange.getMinValidKey());
        storageReport.setMaxPath(validKeyRange.getMaxValidKey());
        vmReportUpdater.accept(vmReport, storageReport);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @NotNull
    private static StorageReport createStoreReport(DataFileCollection dfc, Function<ReadableSequentialData, ?> deser) {
        LongList itemCountByPath = new LongListHeap(50_000_000, CONFIGURATION);
        List<DataFileReader> readers = dfc.getAllCompletedFiles();
        AtomicLong totalDataItemCount = new AtomicLong();

        AtomicInteger duplicateItemCount = new AtomicInteger();
        AtomicInteger failure = new AtomicInteger();
        AtomicInteger itemCount = new AtomicInteger();
        AtomicInteger fileCount = new AtomicInteger();
        AtomicInteger sizeInMb = new AtomicInteger();
        AtomicLong wastedSpaceInBytes = new AtomicLong();

        Consumer<DataFileReader> dataFileProcessor = d -> {
            DataFileIterator dataIterator;
            fileCount.incrementAndGet();
            double currentSizeInMb = d.getPath().toFile().length() * BYTES_TO_MEBIBYTES;
            sizeInMb.addAndGet((int) currentSizeInMb);
            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream(1024);
            try {
                dataIterator = d.createIterator();
                log.info("Reading from file: " + d.getIndex() + ", data items = "
                        + d.getMetadata().getDataItemCount());
                totalDataItemCount.addAndGet(d.getMetadata().getDataItemCount());

                while (dataIterator.next()) {
                    try {
                        //int itemSize = dataIterator.getDataItemData().remaining();
                        int itemSize = 0;
                        final long path;
                        Object dataItemData = deser.apply(dataIterator.getDataItemData());
                        if (dataItemData instanceof VirtualHashRecord hashRecord) {
                            itemSize = hashRecord.hash().getSerializedLength() + /*path*/ Long.BYTES;
                            path = hashRecord.path();
                        } else if (dataItemData instanceof VirtualLeafRecord<?, ?> leafRecord) {
                            path = leafRecord.getPath();
                            SerializableDataOutputStream outputStream = new SerializableDataOutputStream(arrayOutputStream);
                            leafRecord.getKey().serialize(outputStream);
                            itemSize += outputStream.size();
                            arrayOutputStream.reset();
                            leafRecord.getValue().serialize(outputStream);
                            itemSize += outputStream.size() + /*path*/ Long.BYTES;
                            arrayOutputStream.reset();
                        } else {
                            throw new UnsupportedOperationException("Unsupported data item type");
                        }
                        long oldVal = itemCountByPath.get(path);
                        if (oldVal > 0) {
                            itemCountByPath.put(path, oldVal + 1);
                            wastedSpaceInBytes.addAndGet(itemSize);
                            if (oldVal == 1) {
                                duplicateItemCount.incrementAndGet();
                            }
                        } else {
                            itemCountByPath.put(path, 1);
                        }
                    } catch (Exception e) {
                        failure.incrementAndGet();
                    } finally {
                        itemCount.incrementAndGet();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        processObjects(readers, dataFileProcessor).join();

        assertEquals(0, failure.get(), "Failure count should be 0");
        assertEquals(totalDataItemCount.get(), itemCount.get(), "Total data item count should match");

        log.info("Leaves found in data files = " + itemCount + ", recreated LongList.size() = "
                + itemCountByPath.size());
        StorageReport storageReport = new StorageReport();
        if (totalDataItemCount.get() > 0 && sizeInMb.get() > 0) {
            storageReport.setWastePercentage(
                    BigDecimal.valueOf(wastedSpaceInBytes.get() * BYTES_TO_MEBIBYTES * 100.0 / sizeInMb.get())
                            .setScale(3, HALF_UP)
                            .doubleValue());
        }
        storageReport.setDuplicateItems(duplicateItemCount.get());
        storageReport.setNumberOfStorageFiles(fileCount.get());
        storageReport.setOnDiskSizeInMb(sizeInMb.get());
        storageReport.setItemCount(itemCount.get());
        return storageReport;
    }
}
