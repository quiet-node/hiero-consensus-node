// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.network.protocol.rpc;

import com.hedera.hapi.platform.message.GossipPing;

record PingReplyCall(GossipPing ping) implements SenderCall {}
