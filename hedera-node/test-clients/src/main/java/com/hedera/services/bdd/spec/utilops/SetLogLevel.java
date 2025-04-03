// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.spec.utilops;

import static java.util.Objects.requireNonNull;

import com.hedera.services.bdd.junit.hedera.NodeSelector;
import com.hedera.services.bdd.junit.hedera.HederaNode;
import com.hedera.services.bdd.junit.hedera.subprocess.SubProcessNetwork;
import com.hedera.services.bdd.spec.HapiSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility operation that sets logging levels for specific classes on specific nodes.
 * This uses JMX to dynamically update the Log4j2 configuration in the node JVM.
 */
public class SetLogLevel extends UtilOp {
    private static final Logger log = LogManager.getLogger(SetLogLevel.class);
    
    private final NodeSelector selector;
    private final Map<String, String> loggingLevels = new HashMap<>();
    
    /**
     * Creates a new operation to set logging levels for all nodes in the network.
     * 
     * @param className the fully qualified class name for which to set the logging level
     * @param level the logging level to set (e.g., "DEBUG", "INFO", "WARN", "ERROR")
     */
    public SetLogLevel(@NonNull final String className, @NonNull final String level) {
        this(null, className, level);
    }
    
    /**
     * Creates a new operation to set logging levels for specific nodes in the network.
     * 
     * @param selector the selector for the nodes to apply logging levels to, or null for all nodes
     * @param className the fully qualified class name for which to set the logging level
     * @param level the logging level to set (e.g., "DEBUG", "INFO", "WARN", "ERROR")
     */
    public SetLogLevel(
            final NodeSelector selector,
            @NonNull final String className,
            @NonNull final String level) {
        this.selector = selector;
        this.loggingLevels.put(requireNonNull(className), requireNonNull(level));
    }
    
    /**
     * Add another class and logging level to this operation.
     * 
     * @param className the fully qualified class name for which to set the logging level
     * @param level the logging level to set (e.g., "DEBUG", "INFO", "WARN", "ERROR")
     * @return this operation for method chaining
     */
    public SetLogLevel andClass(@NonNull final String className, @NonNull final String level) {
        this.loggingLevels.put(requireNonNull(className), requireNonNull(level));
        return this;
    }

    @Override
    protected boolean submitOp(HapiSpec spec) {
        if (!(spec.targetNetworkOrThrow() instanceof SubProcessNetwork)) {
            log.warn("SetLogLevel operation is only supported for subprocess networks, ignoring");
            return false;
        }
        
        SubProcessNetwork network = (SubProcessNetwork) spec.targetNetworkOrThrow();
        
        if (selector == null) {
            // Apply to all nodes
            log.info("Setting log levels for all nodes: {}", loggingLevels);
            network.applyLoggingLevels(loggingLevels);
        } else {
            // Apply to selected nodes
            List<HederaNode> selectedNodes = network.nodesFor(selector);
            log.info("Setting log levels for {} selected nodes: {}", selectedNodes.size(), loggingLevels);
            
            for (HederaNode node : selectedNodes) {
                network.applyLoggingLevelsToNode(node, loggingLevels);
            }
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        if (selector == null) {
            return "SetLogLevel{classes=" + loggingLevels.keySet() + ", network-wide}";
        } else {
            return "SetLogLevel{classes=" + loggingLevels.keySet() + ", selector=" + selector + "}";
        }
    }
} 