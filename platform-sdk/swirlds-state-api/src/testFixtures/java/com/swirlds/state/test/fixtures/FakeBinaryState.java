package com.swirlds.state.test.fixtures;

import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.base.crypto.Hash;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class FakeBinaryState implements BinaryState {
    private Map<Integer, Bytes> singletons = new HashMap<>();
    private Map<Integer, Map<Bytes, Bytes>> keyValuePairs = new HashMap<>();
    private Map<Integer, Queue<Bytes>> queues = new HashMap<>();

    @Override
    public void putSingleton(int id, Bytes value) {
        singletons.put(id, value);
    }

    @Override
    public <T> void putSingleton(int id, Codec<T> codec, T value) {
        singletons.put(id, codec.toBytes(value));
    }

    @Override
    public Bytes getSingleton(int id) {
        return singletons.get(id);
    }

    @Override
    public <T> T getSingleton(int id, Codec<T> codec) {
        try {
            return codec.parse(singletons.get(id));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Bytes removeSingleton(int id) {
        return singletons.remove(id);
    }

    @Override
    public <T> T removeSingleton(int id, Codec<T> codec) {
        try {
            return codec.parse(removeSingleton(id));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putKeyValuePair(int id, Bytes key, Bytes value) {
        Map<Bytes, Bytes> kvMap = getKvMap(id);
        kvMap.put(key, value);
    }

    @Override
    public <K, V> void putKeyValuePair(int id, Codec<K> keyCodec, K key, Codec<V> valueCodec, V value) {
        Map<Bytes, Bytes> kvMap = getKvMap(id);
        Bytes keyBytes = keyCodec.toBytes(key);
        Bytes valueBytes = valueCodec.toBytes(value);
        kvMap.put(keyBytes, valueBytes);
    }

    @Override
    public <K, V> V getValueByKey(int id, Codec<K> keyCodec, K key, Codec<V> valueCodec) {
        Map<Bytes, Bytes> kvMap = getKvMap(id);
        Bytes keyBytes = keyCodec.toBytes(key);
        try {
            return valueCodec.parse(kvMap.get(keyBytes));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Bytes, Bytes> getKvMap(int id) {
        return keyValuePairs.computeIfAbsent(id, k -> new HashMap<>());
    }

    @NonNull
    private Queue<Bytes> getQueue(int id) {
        return queues.computeIfAbsent(id, k -> new LinkedList<>());
    }

    @Override
    public Bytes removeKeyValuePair(int id, Bytes key) {
        return getKvMap(id).remove(key);
    }

    @Override
    public <K, V> V removeKeyValuePair(int id, Codec<K> keyCodec, K key, Codec<V> valueCodec) {
        try {
            return valueCodec.parse(getKvMap(id).remove(keyCodec.toBytes(key)));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Bytes getValueByKey(int id, Bytes key) {
        return getKvMap(id).get(key);
    }

    @Override
    public void queueAdd(int id, Bytes value) {
        getQueue(id).add(value);
    }

    @Override
    public Bytes queuePoll(int id) {
        return getQueue(id).poll();
    }

    @Override
    public Bytes queuePeek(int id) {
        return getQueue(id).peek();
    }



    @Override
    public <T> void queueAdd(int id, Codec<T> codec, T value) {
        getQueue(id).add(codec.toBytes(value));
    }

    @Override
    public <T> T queuePoll(int id, Codec<T> codec) {
        try {
            return codec.parse(getQueue(id).poll());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T queuePeek(int id, Codec<T> codec) {
        try {
            return codec.parse(getQueue(id).peek());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    @Override
    public <T> Iterator<T> createQueueIterator(int id, Codec<T> codec) {
        return getQueue(id).stream().map(v -> {
            try {
                return codec.parse(v);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }).iterator();
    }

    @Override
    public Hash getHash() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHashed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean release() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reserve() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDestroyed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isImmutable() {
        throw new UnsupportedOperationException();
    }
}
