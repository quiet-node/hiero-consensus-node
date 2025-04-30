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

package com.swirlds.merkledb.files;

import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.swirlds.merkledb.collections.ImmutableIndexedObjectList;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataFileCollectionW<D> {
    DataFileCollection dataFileCollection;
    Class<?> clazz = DataFileCollection.class;
    java.lang.reflect.Field dataFiles;
    Function<ReadableSequentialData, D> deserializer;

    public DataFileCollectionW(DataFileCollection dataFileCollection, Function<ReadableSequentialData, D> deserializer) {
        this.dataFileCollection = dataFileCollection;
        this.deserializer = deserializer;

        try {
            this.dataFiles = clazz.getDeclaredField("dataFiles");
            dataFiles.setAccessible(true);
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public D readDataItem(long dataLocation) {
        try {
            BufferedData bufferedData = dataFileCollection.readDataItem(dataLocation);
            return deserializer.apply(bufferedData);
        } catch (IOException e) {
            final int currentFileIndex = DataFileCommon.fileIndexFromDataLocation(dataLocation);
            final String dataLocationString =  DataFileCommon.dataLocationToString(dataLocation);
            final String dataFilesList =
                    getDataFiles().get().stream().map(v -> "index %s, path %s".formatted(v.getIndex(), v.getPath())).collect(Collectors.joining("\n"));
            final String errorMessage = "Error reading data file. dataLocationString = %s, currentFileIndex = %s. \n Full file list: %s"
                    .formatted(dataLocationString, currentFileIndex, dataFilesList); ;
            throw new RuntimeException(errorMessage, e);
        }
    }

    public AtomicReference<ImmutableIndexedObjectList<DataFileReader>> getDataFiles() {
        try {
            return (AtomicReference<ImmutableIndexedObjectList<DataFileReader>>) dataFiles.get(dataFileCollection);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
