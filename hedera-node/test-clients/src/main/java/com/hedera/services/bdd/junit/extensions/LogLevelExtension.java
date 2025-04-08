// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.junit.extensions;

import static com.hedera.services.bdd.junit.extensions.ExtensionUtils.hapiTestMethodOf;
import static com.hedera.services.bdd.junit.hedera.ExternalPath.APPLICATION_LOG;
import static com.hedera.services.bdd.junit.hedera.NodeSelector.byNodeId;

import com.hedera.services.bdd.junit.LogLevel;
import com.hedera.services.bdd.spec.HapiSpec;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * An extension to handle running {@HapiTest} with different log levels.
 */
public class LogLevelExtension implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        hapiTestMethodOf(extensionContext).ifPresent(method -> {
            LogLevel annotation = method.getAnnotation(LogLevel.class);
            if (annotation != null) {
                LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                Configuration config = ctx.getConfiguration();
                LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
                loggerConfig.setLevel(Level.getLevel(annotation.value()));

                LoggerConfig hederaLogger = config.getLoggerConfig("com.hedera");
                hederaLogger.setLevel(Level.getLevel(annotation.value()));

                HapiSpec.TARGET_NETWORK.get().nodesFor(byNodeId(0)).forEach(node -> {
                    LoggerConfig nodeLogger = config.getLoggerConfig(node.getExternalPath(
                                    APPLICATION_LOG /*node.metadata().workingDir().resolve(OUTPUT_DIR).resolve("hgcaa.log")*/)
                            .toString());
                    nodeLogger.setLevel(Level.getLevel(annotation.value()));
                });

                ctx.updateLoggers();
            }
        });
    }

    @Override
    public void afterEach(ExtensionContext context) {
        // Reset to default level
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.INFO);
        ctx.updateLoggers();
    }
}
