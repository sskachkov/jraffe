package io.github.sskachkov.jraffe.core.rpc;

public sealed interface RpcPayload permits AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse {}
