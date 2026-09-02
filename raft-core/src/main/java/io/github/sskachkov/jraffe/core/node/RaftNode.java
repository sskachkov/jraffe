package io.github.sskachkov.jraffe.core.node;

import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.core.rpc.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface RaftNode {
    RaftResponse submitSync(RaftMessage request);

    RaftResponse submitReadonlySync(RaftMessage request);

    CompletableFuture<RaftResponse> submit(RaftMessage request);

    CompletableFuture<RaftResponse> submitReadonly(RaftMessage request);

    RpcEnvelope<RequestVoteResponse> handleRequestVote(RpcEnvelope<RequestVoteRequest> reqEnv);

    RpcEnvelope<AppendEntriesResponse> handleAppendEntries(RpcEnvelope<AppendEntriesRequest> reqEnv);

    void start();

    void stop();

    Role getRole();

    String getId();

    long getCurrentTerm();

    long getCommitIndex();

    List<String> getPeerIds();

    String getLeaderId();

    Optional<Map<String, PeerReplicationStatus>> getReplicationStatus();
}
