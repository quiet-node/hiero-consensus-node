// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.network.protocol.rpc;

import com.hedera.hapi.platform.event.GossipEvent;
import java.util.List;

record EventsCall(List<GossipEvent> gossipEvents) implements SenderCall {}
