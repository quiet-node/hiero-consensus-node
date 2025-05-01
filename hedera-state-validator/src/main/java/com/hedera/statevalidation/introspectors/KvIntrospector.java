package com.hedera.statevalidation.introspectors;

import com.hedera.pbj.runtime.JsonCodec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.State;
import com.swirlds.state.spi.ReadableKVState;

import static com.hedera.statevalidation.introspectors.IntrospectUtils.getCodecFor;

public class KvIntrospector {

    private final State state;
    private final String serviceName;
    private final String stateName;
    private final String keyAsJson;

    public KvIntrospector(State state, String serviceName, String stateName, String keyAsJson) {
        this.state = state;
        this.serviceName = serviceName;
        this.stateName = stateName;
        this.keyAsJson = keyAsJson;
    }

    public void introspect() {
        ReadableKVState<Object, Object> kvState = state.getReadableStates(serviceName).get(stateName);

        Object next = kvState.keys().next();
        JsonCodec jsonCodec = getCodecFor(next);
        try {
            Object key = jsonCodec.parse(Bytes.wrap(keyAsJson));
            Object value = kvState.get(key);
            if (value == null) {
                System.out.println("Value not found");
            } else {
                System.out.println(getCodecFor(value).toJSON(value));
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
