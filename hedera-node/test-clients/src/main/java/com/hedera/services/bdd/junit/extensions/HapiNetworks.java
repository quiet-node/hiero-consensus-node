// SPDX-License-Identifier: Apache-2.0
package com.hedera.services.bdd.junit.extensions;

import static com.hedera.services.bdd.junit.hedera.subprocess.SubProcessNetwork.SHARED_NETWORK_NAME;

import com.hedera.services.bdd.junit.hedera.BlockNodeNetwork;
import com.hedera.services.bdd.junit.hedera.HederaNetwork;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import edu.umd.cs.findbugs.annotations.Nullable;

public class HapiNetworks {
	private static final Logger log = LogManager.getLogger(HapiNetworks.class);

	private static final AtomicReference<Map<String, HederaNetwork>> ALL_NETWORKS = new AtomicReference<>(new HashMap<>());

	// (FUTURE) Encapsulate
	public static final AtomicReference<BlockNodeNetwork> SHARED_BLOCK_NODE_NETWORK = new AtomicReference<>();

	public static HederaNetwork sharedNetwork() {
		return getNetwork(SHARED_NETWORK_NAME);
	}

	public static void terminateAll() {
		ALL_NETWORKS.get().forEach((name, network) -> {
			if (network != null) {
				network.terminate();
			}
		});
	}

	public static boolean isRegistered(@Nullable HederaNetwork network) {
		return network != null && ALL_NETWORKS.get().containsValue(network);
	}

	public static HederaNetwork getNetwork(@Nullable String name) {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("Network name cannot be null or empty");
		}
		return ALL_NETWORKS.get().get(name);
	}

	public static void setSharedNetwork(@Nullable final HederaNetwork network) {
		setNetwork(SHARED_NETWORK_NAME, network);
	}

	public static void setNetwork(String name, @Nullable final HederaNetwork network) {
		if (ALL_NETWORKS.get().containsKey(name)) {
			throw new IllegalStateException(
					"Network for " + name + " already set!");
		}

		ALL_NETWORKS.get().put(name, network);

		ALL_NETWORKS.get().forEach((k,v) -> {
			log.info("Network registered: {} -> {}", k, v == null ? "null" : v.name());
		});
	}
}
