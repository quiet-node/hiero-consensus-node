package com.hedera.statevalidation.validator;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectPackage;

import com.hedera.statevalidation.listener.LoggingTestExecutionListener;
import com.hedera.statevalidation.listener.ReportingListener;
import com.hedera.statevalidation.listener.SummaryGeneratingListener;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.concurrent.Callable;

/**
 * This class is the base class for all validators.
 * It is responsible for discovering all tests in the package and running them. Uses provided tags to filter tests.<br>
 * All validators are expecting 2 parameters:<br>
 * 1. State directory - the directory where the state is stored<br>
 * 2. Tag to run - the tag of the test to run (optional) If no tags are provided, all tests are run.<br>
 */
public class ValidatorBase implements Callable<Integer> {
    @Parameters(index = "0", description = "State directory")
    private File stateDir;
    @Parameters(arity = "1..*", index = "1", description = "Tag to run: [stateAnalyzer, internal, leaf, hdhm, account, tokenRelations, rehash, files, compaction]")
    private String[] tags = {"stateAnalyzer", "internal", "leaf", "hdhm", "account", "tokenRelations", "rehash", "files", "compaction"};

    @Override
    public Integer call() {
        System.setProperty("state.dir", stateDir.getAbsolutePath());

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectPackage("com.hedera.statevalidation.validators"))
                .filters(TagFilter.includeTags(tags))
                .build();

        TestPlan testPlan;
        SummaryGeneratingListener summaryGeneratingListener = new SummaryGeneratingListener();
        try (LauncherSession session = LauncherFactory.openSession()) {
            Launcher launcher = session.getLauncher();
            launcher.registerTestExecutionListeners(new ReportingListener(), summaryGeneratingListener, new LoggingTestExecutionListener());
            testPlan = launcher.discover(request);
            launcher.execute(testPlan);
        }

        return summaryGeneratingListener.isFailed() ? 1 : 0;
    }
}
