package com.hedera.statevalidation.validators.merkledb;

import com.swirlds.merkledb.MerkleDbDataSourceW;
import com.hedera.statevalidation.parameterresolver.ReportResolver;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceProvider;
import com.hedera.statevalidation.parameterresolver.VirtualMapAndDataSourceRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@ExtendWith({ReportResolver.class})
@Tag("compaction")
public class Compaction {

    @ParameterizedTest
    @ArgumentsSource(VirtualMapAndDataSourceProvider.class)
    void compaction(VirtualMapAndDataSourceRecord labelAndDs) {
        MerkleDbDataSourceW vds = labelAndDs.createMerkleDSWrapper();
        vds.getCompactionCoordinator().runCompaction();
    }

}
