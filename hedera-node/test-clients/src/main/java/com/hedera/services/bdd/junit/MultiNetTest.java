// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.junit;

import static com.hedera.services.bdd.junit.TestTags.ONLY_SUBPROCESS;

import com.hedera.services.bdd.junit.extensions.NetworkTargetingExtension;
import com.hedera.services.bdd.junit.extensions.SpecNamingExtension;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * TODO
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@TestFactory
@Tag(ONLY_SUBPROCESS)
@ExtendWith({NetworkTargetingExtension.class, SpecNamingExtension.class})
@Isolated
public @interface MultiNetTest {}
