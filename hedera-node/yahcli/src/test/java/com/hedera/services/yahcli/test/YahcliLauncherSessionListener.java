package com.hedera.services.yahcli.test;

import com.hedera.services.bdd.junit.HapiTest;
import com.hedera.services.bdd.junit.LeakyHapiTest;
import com.hedera.services.bdd.junit.SharedNetworkLauncherSessionListener;
import com.hedera.services.bdd.junit.support.TestPlanUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.util.Set;

import static com.hedera.services.bdd.junit.support.TestPlanUtils.hasAnnotatedTestNode;

public class YahcliLauncherSessionListener implements LauncherSessionListener {
    private static final Logger log = LogManager.getLogger(YahcliLauncherSessionListener.class);

    @Override
    public void launcherSessionOpened(@NonNull final LauncherSession session) {
        session.getLauncher().registerTestExecutionListeners(new YahcliExecutionListener());
    }

    public static class YahcliExecutionListener implements TestExecutionListener {
        @Override
        public void testPlanExecutionStarted(@NonNull final TestPlan testPlan) {
            // Only do extra setup if it's possible we're targeting a SubprocessNetwork
            if (!hasAnnotatedTestNode(testPlan, Set.of(HapiTest.class, LeakyHapiTest.class))) {
                return;
            }
            SharedNetworkLauncherSessionListener.onSubProcessNetworkReady(network -> {
                log.info("YahcliLauncherSessionListener: SubProcessNetwork is ready for Yahcli tests, {}", network.nodes());
            });
        }
    }
}
