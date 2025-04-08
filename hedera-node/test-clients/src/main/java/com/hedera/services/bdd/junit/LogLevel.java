// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.junit;

import com.hedera.services.bdd.junit.extensions.LogLevelExtension;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({LogLevelExtension.class})
public @interface LogLevel {
    /**
     * @return the log level in which the test has to run
     */
    String value() default "INFO";
}
