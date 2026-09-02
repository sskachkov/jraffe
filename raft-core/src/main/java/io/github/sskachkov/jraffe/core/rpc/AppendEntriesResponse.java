package io.github.sskachkov.jraffe.core.rpc;

public record AppendEntriesResponse(long term, boolean success) implements RpcPayload {}
