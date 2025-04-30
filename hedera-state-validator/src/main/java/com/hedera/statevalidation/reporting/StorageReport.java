package com.hedera.statevalidation.reporting;

import lombok.Data;

@Data
public class StorageReport {
    long minPath;
    long maxPath;

    long onDiskSizeInMb;
    long numberOfStorageFiles;

    double wastePercentage;
    int duplicateItems;
    long itemCount;

}
