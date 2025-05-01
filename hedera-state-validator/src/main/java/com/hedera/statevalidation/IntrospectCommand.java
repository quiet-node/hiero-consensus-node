package com.hedera.statevalidation;

import com.hedera.statevalidation.introspectors.KvIntrospector;
import com.hedera.statevalidation.introspectors.SingletonIntrospector;
import com.hedera.statevalidation.parameterresolver.StateResolver;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.snapshot.DeserializedSignedState;
import com.swirlds.state.State;
import com.swirlds.state.spi.ReadableKVState;
import picocli.CommandLine;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;

import static picocli.CommandLine.*;

@Command(name = "introspect", description = "Introspects the state")
public class IntrospectCommand implements Runnable{

    @ParentCommand
    private StateOperatorCommand parent;

    @Parameters(index = "0", description = "Service name")
    private String serviceName;

    @Parameters(index = "1", description = "State name")
    private String stateName;

    @Parameters(index = "2", arity = "0..1", description = "Key as JSON")
    private String keyAsJson;

    @Override
    public void run() {
        System.setProperty("state.dir", parent.getStateDir().getAbsolutePath());

        State state;
        try {
            DeserializedSignedState deserializedSignedState = StateResolver.initState();
            state = deserializedSignedState.reservedSignedState().get().getState();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(keyAsJson == null) {
            // we assume it's a singleton
            final SingletonIntrospector introspector = new SingletonIntrospector(state, serviceName, stateName);
            introspector.introspect();
        } else {
            final KvIntrospector introspector = new KvIntrospector(state, serviceName, stateName, keyAsJson);
            introspector.introspect();
        }

    }
}
