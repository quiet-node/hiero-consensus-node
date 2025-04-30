package com.hedera.statevalidation.listener;

import static com.hedera.statevalidation.listener.ListenerUtils.extractTestFullName;

import lombok.extern.log4j.Log4j2;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

/**
 * Logs the start and end of each test. Helps to see clearly the log boundaries of each test.
 */
@Log4j2
public class LoggingTestExecutionListener implements TestExecutionListener {
    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        if (testIdentifier.isTest()) {
            log.debug(framedString(extractTestFullName(testIdentifier) + " started"));
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        if (testIdentifier.isTest()) {
            log.debug(framedString(extractTestFullName(testIdentifier) + " finished"));
        }
    }

    private String framedString(String stringToFrame) {
        String frame = " ".repeat(stringToFrame.length() + 6);
        return String.format("\n%s\n   %s   \n%s", frame, stringToFrame, frame);
    }
}
