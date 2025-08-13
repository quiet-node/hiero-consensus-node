// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.config.data;

import com.hedera.node.config.NodeProperty;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;

/**
 * Configuration for the contextual ISS information the Execution layer will write to disk.
 */
@ConfigData("issContextConfig")
public record IssContextConfig(
        @ConfigProperty(defaultValue = "true") @NodeProperty boolean enabled,
        @ConfigProperty(defaultValue = "output/iss/") @NodeProperty String diskPath,
        @ConfigProperty(defaultValue = "5") @Min(5) @Max(100) @NodeProperty int recordBlockBufferSize) {}
