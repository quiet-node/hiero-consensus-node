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

package com.swirlds.merkledb;

import com.swirlds.merkledb.collections.HashList;
import com.swirlds.merkledb.collections.LongList;
import com.swirlds.merkledb.files.MemoryIndexDiskKeyValueStore;
import com.swirlds.merkledb.files.hashmap.HalfDiskHashMap;

import java.lang.reflect.Field;

public class MerkleDbDataSourceW {
    private final MerkleDbDataSource dataSource;
    Class<?> clazz = MerkleDbDataSource.class;

    // indexes path -> diskLocation
    // this is the only index needed for internal nodes
    Field hashPathToDiskLocation; // LongList
    Field leafPathToDiskLocation; // LongList

    // indexes for leaf nodes key->path, only one of them is used

    Field keyToPath; // HalfDiskHashMap<K>

    // internal nodes store
    Field pathToHashRam; // HashList
    Field pathToHashDisk; // MemoryIndexDiskKeyValueStore<VirtualInternalRecord>
    // leaf node store
    Field pathToKeyValue; // MemoryIndexDiskKeyValueStore<VirtualLeafRecord<K, V>>

    MerkleDbCompactionCoordinatorW compactionCoordinator;

    public MerkleDbDataSourceW(MerkleDbDataSource dataSource) {
        this.dataSource = dataSource;
        this.compactionCoordinator = new MerkleDbCompactionCoordinatorW(dataSource);
        this.hashPathToDiskLocation = getField("pathToDiskLocationInternalNodes");
        this.leafPathToDiskLocation = getField("pathToDiskLocationLeafNodes");
        this.keyToPath = getField("keyToPath");
        this.pathToHashRam = getField("hashStoreRam");
        this.pathToHashDisk = getField("hashStoreDisk");
        this.pathToKeyValue = getField("pathToKeyValue");
    }

    private Field getField(String fieldName) {
        try {
            var field = this.clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public LongList getHashPathToDiskLocation() {
        try {
            return (LongList) hashPathToDiskLocation.get(dataSource);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public LongList getLeafPathToDiskLocation() {
        try {
            return (LongList) leafPathToDiskLocation.get(dataSource);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public HalfDiskHashMap getKeyToPath() {
        try {
            return (HalfDiskHashMap) keyToPath.get(dataSource);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public HashList getPathToHashRam() {
        try {
            return (HashList) pathToHashRam.get(dataSource);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public MemoryIndexDiskKeyValueStore getPathToHashDisk() {
        try {
            return (MemoryIndexDiskKeyValueStore) pathToHashDisk.get(dataSource);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public MemoryIndexDiskKeyValueStore getPathToKeyValue() {
        try {
            return (MemoryIndexDiskKeyValueStore) pathToKeyValue.get(dataSource);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public long getHashesRamToDiskThreshold() {
        return dataSource.getHashesRamToDiskThreshold();
    }

    public MerkleDbCompactionCoordinatorW getCompactionCoordinator() {
        return compactionCoordinator;
    }
}
