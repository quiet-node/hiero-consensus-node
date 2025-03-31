package com.swirlds.common.poc.impl;

import java.time.Duration;

public interface Node {
    void kill(Duration timeout);

    void revive(Duration timeout);
}
