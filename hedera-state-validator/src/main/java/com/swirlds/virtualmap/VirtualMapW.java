package com.swirlds.virtualmap;

import com.swirlds.base.function.CheckedSupplier;
import com.swirlds.virtualmap.datasource.VirtualDataSource;
import com.swirlds.virtualmap.serialize.KeySerializer;
import com.swirlds.virtualmap.serialize.ValueSerializer;

public class VirtualMapW<K extends VirtualKey, V extends VirtualValue> {

    private final VirtualMap<K, V> virtualMap;
    private final KeySerializer<K> keySerializer;
    private final ValueSerializer<V> valueSerializer;

    public VirtualMapW(VirtualMap<K, V> virtualMap,
                       KeySerializer<K> keySerializer,
                       ValueSerializer<V> valueSerializer) {
        this.virtualMap = virtualMap;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public VirtualValue get(K key) {
        return virtualMap.get(key);
    }

    public VirtualDataSource getDataSource() {
        return virtualMap.getDataSource();
    }

    public static <K extends VirtualKey, V extends VirtualValue> VirtualMapW<K, V> wrap(VirtualMap<K, V> virtualMap,
                                                                                        KeySerializer<K> keySerializer,
                                                                                        ValueSerializer<V> valueSerializer) {
        return new VirtualMapW<>(virtualMap, keySerializer, valueSerializer);
    }

    public KeySerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public ValueSerializer<V> getValueSerializer() {
        return valueSerializer;
    }
}
