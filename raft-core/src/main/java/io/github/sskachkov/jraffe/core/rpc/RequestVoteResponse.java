package io.github.sskachkov.jraffe.core.rpc;

public record RequestVoteResponse(long term, boolean voteGranted) implements RpcPayload {}
