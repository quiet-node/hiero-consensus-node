package org.hiero.otter.fixtures.result;

import com.swirlds.logging.legacy.payload.LogPayload;

@FunctionalInterface
public interface ReconnectSubscriber<T extends LogPayload> {

    T onPayload(T payload);
}
