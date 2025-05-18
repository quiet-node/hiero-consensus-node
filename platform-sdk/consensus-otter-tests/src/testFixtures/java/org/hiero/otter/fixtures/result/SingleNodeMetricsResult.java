// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.result;

import java.util.List;
import org.hiero.consensus.model.node.NodeId;

public interface SingleNodeMetricsResult {
    NodeId nodeId();

    String category();

    String name();

    List<Object> history();
}
