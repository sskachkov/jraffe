package io.github.sskachkov.jraffe.core.rpc;

public record RequestVoteRequest(long term, long lastLogIndex, long lastLogTerm) implements RpcPayload {}
