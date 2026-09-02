package io.github.sskachkov.jraffe.core.rpc;

public record RpcEnvelope<T>(String sender, String recipient, long sentAt, long requestId, long correlationId, T payload) {}
