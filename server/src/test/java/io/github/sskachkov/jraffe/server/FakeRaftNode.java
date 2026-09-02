package io.github.sskachkov.jraffe.server;

import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.core.node.PeerReplicationStatus;
import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.node.Role;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesRequest;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesResponse;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteRequest;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteResponse;
import io.github.sskachkov.jraffe.core.rpc.RpcEnvelope;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

// Fully-controllable RaftNode test double: no threading, no real Raft state, just whatever
// each test wires up beforehand via the public fields/functions.
public class FakeRaftNode implements RaftNode {
    public RaftResponse submitSyncResponse;
    public RaftResponse submitReadonlySyncResponse;
    public Role role = Role.FOLLOWER;
    public String id = "n1";
    public long currentTerm;
    public long commitIndex;
    public List<String> peerIds = List.of();
    public String leaderId;
    public Optional<Map<String, PeerReplicationStatus>> replicationStatus = Optional.empty();
    public Function<RpcEnvelope<RequestVoteRequest>, RpcEnvelope<RequestVoteResponse>> onRequestVote;
    public Function<RpcEnvelope<AppendEntriesRequest>, RpcEnvelope<AppendEntriesResponse>> onAppendEntries;

    @Override
    public RaftResponse submitSync(RaftMessage request) {
        return submitSyncResponse;
    }

    @Override
    public RaftResponse submitReadonlySync(RaftMessage request) {
        return submitReadonlySyncResponse;
    }

    @Override
    public CompletableFuture<RaftResponse> submit(RaftMessage request) {
        throw new UnsupportedOperationException("not used by these tests");
    }

    @Override
    public CompletableFuture<RaftResponse> submitReadonly(RaftMessage request) {
        throw new UnsupportedOperationException("not used by these tests");
    }

    @Override
    public RpcEnvelope<RequestVoteResponse> handleRequestVote(RpcEnvelope<RequestVoteRequest> reqEnv) {
        if (onRequestVote == null) {
            throw new UnsupportedOperationException("onRequestVote not wired up");
        }
        return onRequestVote.apply(reqEnv);
    }

    @Override
    public RpcEnvelope<AppendEntriesResponse> handleAppendEntries(RpcEnvelope<AppendEntriesRequest> reqEnv) {
        if (onAppendEntries == null) {
            throw new UnsupportedOperationException("onAppendEntries not wired up");
        }
        return onAppendEntries.apply(reqEnv);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public Role getRole() {
        return role;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public long getCommitIndex() {
        return commitIndex;
    }

    @Override
    public List<String> getPeerIds() {
        return peerIds;
    }

    @Override
    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public Optional<Map<String, PeerReplicationStatus>> getReplicationStatus() {
        return replicationStatus;
    }
}
