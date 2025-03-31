package com.swirlds.common.poc.impl;

public interface TestEnvironment {
    Network network();

    TimeManager timeManager();

    EventGenerator generator();

    Validator validator();
}
