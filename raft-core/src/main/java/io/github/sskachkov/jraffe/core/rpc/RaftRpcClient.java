package io.github.sskachkov.jraffe.core.rpc;

import java.util.List;

/**
 * The peer-to-peer RPC contract a RaftNode needs — RequestVote and AppendEntries, plus their
 * request/response types. Implementations (gRPC, Maelstrom stdio, in-memory for tests) supply
 * the actual transport.
 */
public interface RaftRpcClient {
    RpcEnvelope<RequestVoteResponse> requestVote(RpcEnvelope<RequestVoteRequest> request) throws RaftRpcException;
    RpcEnvelope<AppendEntriesResponse> appendEntries(RpcEnvelope<AppendEntriesRequest> request) throws RaftRpcException;
}
