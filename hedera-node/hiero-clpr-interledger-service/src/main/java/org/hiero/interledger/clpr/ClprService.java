// SPDX-License-Identifier: Apache-2.0
package org.hiero.interledger.clpr;

import com.hedera.node.app.spi.RpcService;
import com.hedera.node.app.spi.RpcServiceFactory;
import com.hedera.pbj.runtime.RpcServiceDefinition;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ServiceLoader;
import java.util.Set;
import org.hiero.hapi.interledger.clpr.ClprServiceInterface;

/**
 * Implements the HAPI <a
 * href="https://github.com/hashgraph/hedera-protobufs/blob/main/interledger/clpr_service.proto">CLPR
 * Service</a>.
 */
public interface ClprService extends RpcService {
    /**
     * The name of the service.
     */
    String NAME = ClprServiceInterface.SERVICE_NAME;

    @NonNull
    @Override
    default String getServiceName() {
        return NAME;
    }

    @NonNull
    @Override
    default Set<RpcServiceDefinition> rpcDefinitions() {
        return Set.of(ClprServiceDefinition.INSTANCE);
    }

    /**
     * Returns the concrete implementation instance of the service.
     *
     * @return the implementation instance
     */
    @NonNull
    static ClprService getInstance() {
        return RpcServiceFactory.loadService(ClprService.class, ServiceLoader.load(ClprService.class));
    }
}
