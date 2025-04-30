package com.hedera.statevalidation.validators;

import com.swirlds.merkledb.collections.ImmutableIndexedObjectList;
import com.swirlds.merkledb.files.DataFileCollectionW;
import com.swirlds.merkledb.files.DataFileReader;
import org.apache.logging.log4j.Logger;

import static com.swirlds.merkledb.files.DataFileCommon.dataLocationToString;

public final class Utils {
    private Utils() {}

    @SuppressWarnings("StringConcatenationArgumentToLogCall")
    public static void printFileDataLocationError(Logger logger, String message, DataFileCollectionW dfc, long dataLocation) {
        final ImmutableIndexedObjectList<DataFileReader> activeIndexedFiles = (ImmutableIndexedObjectList<DataFileReader>) dfc.getDataFiles().get();
        logger.error("Error! Details: " + message);
        logger.error("Data location: " + dataLocationToString(dataLocation));
        logger.error("Data file collection: ");
        activeIndexedFiles.stream().forEach(a -> {
            logger.error("File: " + a.getPath());
            logger.error("Size: " + a.getSize());
            logger.error("Metadata: " + a.getMetadata());
        });
    }
}
