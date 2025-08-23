// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.cli.utils;

import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.metrics.noop.NoOpMetrics;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.service.PlatformStateFacade;
import com.swirlds.platform.state.snapshot.DeserializedSignedState;
import com.swirlds.platform.system.SwirldMain;
import com.swirlds.virtualmap.VirtualMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.hiero.base.crypto.Hash;

public class HederaAppUtils {
    public static MerkleNodeState createrNewMerkleNodeState(VirtualMap virtualMap) {
        try {
            Class<?> stateClass = Class.forName("com.hedera.node.app.HederaVirtualMapState");
            Constructor<?> constructor = stateClass.getConstructor(VirtualMap.class);
            return (MerkleNodeState) constructor.newInstance(virtualMap);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException
                | InstantiationException
                | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static SwirldMain<?> createHederaApp(
            PlatformContext platformContext, PlatformStateFacade platformStateFacade, SwirldMain<?> appMain) {
        final SwirldMain<?> hederaApp;
        Method newHederaMethod;
        try {
            newHederaMethod = appMain.getClass()
                    .getDeclaredMethod("newHedera", Metrics.class, PlatformStateFacade.class, Configuration.class);
            hederaApp = (SwirldMain<?>) newHederaMethod.invoke(
                    null, new NoOpMetrics(), platformStateFacade, platformContext.getConfiguration());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return hederaApp;
    }

    public static void updateStateHash(SwirldMain<?> hederaApp, DeserializedSignedState deserializedSignedState) {
        try {
            Method setInitialStateHash = hederaApp.getClass().getDeclaredMethod("setInitialStateHash", Hash.class);
            setInitialStateHash.invoke(hederaApp, deserializedSignedState.originalHash());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
