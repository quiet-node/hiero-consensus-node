// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.network.protocol.rpc;

import com.swirlds.platform.gossip.rpc.SyncData;

record SyncDataCall(SyncData syncMessage) implements SenderCall {}
