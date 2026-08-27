package io.github.sskachkov.jraffe.core.rpc;

import java.util.List;

/**
 * The peer-to-peer RPC contract a RaftNode needs — RequestVote and AppendEntries, plus their
 * request/response types. Implementations (gRPC, Maelstrom stdio, in-memory for tests) supply
 * the actual transport.
 */
public interface RaftRpcClient {

    record RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {}
    record RequestVoteResponse(long term, boolean voteGranted) {}
    record AppendEntriesRequest(long reqId, long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                 List<LogEntry> entries, long leaderCommit) {}
    record AppendEntriesResponse(long reqId, long term, boolean success) {}

    RequestVoteResponse requestVote(String peerId, RequestVoteRequest request) throws RaftRpcException;
    AppendEntriesResponse appendEntries(String peerId, AppendEntriesRequest request) throws RaftRpcException;
}