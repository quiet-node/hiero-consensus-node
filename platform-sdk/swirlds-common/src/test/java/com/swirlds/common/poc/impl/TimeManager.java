package com.swirlds.common.poc.impl;

import java.time.Duration;

public interface TimeManager {
    void waitFor(Duration waitTime);

    void waitForEvents(int eventCount);
}
