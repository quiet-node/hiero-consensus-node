// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.token.impl.test.handlers.util;

import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NftID;
import com.hedera.hapi.node.base.PendingAirdropId;
import com.hedera.hapi.node.base.TokenID;
import com.hedera.hapi.node.state.common.EntityIDPair;
import com.hedera.hapi.node.state.primitives.ProtoBytes;
import com.hedera.hapi.node.state.token.Account;
import com.hedera.hapi.node.state.token.AccountPendingAirdrop;
import com.hedera.hapi.node.state.token.Nft;
import com.hedera.hapi.node.state.token.Token;
import com.hedera.hapi.node.state.token.TokenRelation;
import com.hedera.node.app.service.token.TokenService;
import com.swirlds.state.test.fixtures.MapReadableKVState;
import com.swirlds.state.test.fixtures.MapWritableKVState;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utility class for building state for tests.
 *
 */
public class StateBuilderUtil {
    /**
     * The state key for accounts.
     */
    public static final String ACCOUNTS = "ACCOUNTS";
    /**
     * The state key for pending airdrops.
     */
    public static final String AIRDROPS = "PENDING_AIRDROPS";
    /**
     * The state key for aliases.
     */
    public static final String ALIASES = "ALIASES";
    /**
     * The state key for tokens.
     */
    public static final String TOKENS = "TOKENS";
    /**
     * The state key for token relations.
     */
    public static final String TOKEN_RELS = "TOKEN_RELS";
    /**
     * The state key for NFTs.
     */
    public static final String NFTS = "NFTS";
    /**
     * The state key for staking infos.
     */
    public static final String STAKING_INFO = "STAKING_INFOS";
    /**
     * The state key for network rewards.
     */
    public static final String NETWORK_REWARDS = "STAKING_NETWORK_REWARDS";

    @NonNull
    protected MapReadableKVState.Builder<AccountID, Account> emptyReadableAccountStateBuilder() {
        return MapReadableKVState.builder(TokenService.NAME, ACCOUNTS);
    }

    @NonNull
    protected MapWritableKVState.Builder<AccountID, Account> emptyWritableAccountStateBuilder() {
        return MapWritableKVState.builder(TokenService.NAME, ACCOUNTS);
    }

    @NonNull
    protected MapReadableKVState.Builder<PendingAirdropId, AccountPendingAirdrop> emptyReadableAirdropStateBuilder() {
        return MapReadableKVState.builder(TokenService.NAME, AIRDROPS);
    }

    @NonNull
    protected MapWritableKVState.Builder<PendingAirdropId, AccountPendingAirdrop> emptyWritableAirdropStateBuilder() {
        return MapWritableKVState.builder(TokenService.NAME, AIRDROPS);
    }

    @NonNull
    protected MapReadableKVState.Builder<EntityIDPair, TokenRelation> emptyReadableTokenRelsStateBuilder() {
        return MapReadableKVState.builder(TokenService.NAME, TOKEN_RELS);
    }

    @NonNull
    protected MapWritableKVState.Builder<EntityIDPair, TokenRelation> emptyWritableTokenRelsStateBuilder() {
        return MapWritableKVState.builder(TokenService.NAME, TOKEN_RELS);
    }

    @NonNull
    protected MapReadableKVState.Builder<NftID, Nft> emptyReadableNftStateBuilder() {
        return MapReadableKVState.builder(TokenService.NAME, NFTS);
    }

    @NonNull
    protected MapWritableKVState.Builder<NftID, Nft> emptyWritableNftStateBuilder() {
        return MapWritableKVState.builder(TokenService.NAME, NFTS);
    }

    @NonNull
    protected MapReadableKVState.Builder<TokenID, Token> emptyReadableTokenStateBuilder() {
        return MapReadableKVState.builder(TokenService.NAME, TOKENS);
    }

    @NonNull
    protected MapWritableKVState.Builder<TokenID, Token> emptyWritableTokenStateBuilder() {
        return MapWritableKVState.builder(TokenService.NAME, TOKENS);
    }

    @NonNull
    protected MapWritableKVState.Builder<ProtoBytes, AccountID> emptyWritableAliasStateBuilder() {
        return MapWritableKVState.builder(TokenService.NAME, ALIASES);
    }

    @NonNull
    protected MapReadableKVState.Builder<ProtoBytes, AccountID> emptyReadableAliasStateBuilder() {
        return MapReadableKVState.builder(TokenService.NAME, ALIASES);
    }

    @NonNull
    protected MapWritableKVState<TokenID, Token> emptyWritableTokenState() {
        return MapWritableKVState.<TokenID, Token>builder(TokenService.NAME, TOKENS)
                .build();
    }
}
