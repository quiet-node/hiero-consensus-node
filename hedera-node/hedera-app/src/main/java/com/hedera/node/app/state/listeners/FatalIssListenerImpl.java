// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.state.listeners;

import static java.util.Objects.requireNonNull;

import com.hedera.node.app.blocks.BlockStreamManager;
import com.hedera.node.app.cache.ExecutionOutputCache;
import com.swirlds.platform.system.state.notifications.AsyncFatalIssListener;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.consensus.model.notification.IssNotification;

@Singleton
public class FatalIssListenerImpl implements AsyncFatalIssListener {

    private static final Logger log = LogManager.getLogger(FatalIssListenerImpl.class);

    private final BlockStreamManager blockStreamManager;

    private final ExecutionOutputCache executionOutputCache;

    @Inject
    public FatalIssListenerImpl(
            @NonNull final ExecutionOutputCache executionOutputCache, @NonNull BlockStreamManager blockStreamManager) {
        this.executionOutputCache = requireNonNull(executionOutputCache);
        this.blockStreamManager = requireNonNull(blockStreamManager);
    }

    @Override
    public void notify(@NonNull final IssNotification data) {
        log.warn("ISS detected (type={}, round={})", data.getIssType(), data.getRound());
        blockStreamManager.notifyFatalEvent();
        executionOutputCache.setIssRoundNumber(data.getRound());
    }
}
