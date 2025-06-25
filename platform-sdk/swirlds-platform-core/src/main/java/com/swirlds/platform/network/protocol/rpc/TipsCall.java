// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.network.protocol.rpc;

import java.util.List;

record TipsCall(List<Boolean> tips) implements SenderCall {}
