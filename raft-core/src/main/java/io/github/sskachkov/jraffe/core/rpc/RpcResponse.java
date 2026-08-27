package io.github.sskachkov.jraffe.core.rpc;

/** Pairs an RPC response with the id of the peer that sent it. */
public record RpcResponse<T>(String peerId, T response) {}