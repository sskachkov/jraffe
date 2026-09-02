package io.github.sskachkov.jraffe.core.rpc;

import java.util.concurrent.atomic.AtomicLong;

public class EnvelopeFactory {
    private final AtomicLong reqIdCounter = new AtomicLong();

    public long lastUsedReqId() {
        return this.reqIdCounter.get();
    }

    public <T extends RpcPayload> RpcEnvelope<T> create(String sender, String recipient, T payload) {
        long reqId = reqIdCounter.incrementAndGet();
        return new RpcEnvelope<>(sender, recipient, System.currentTimeMillis(), reqId, -1, payload);
    }

    public <T extends RpcPayload> RpcEnvelope<T> response(RpcEnvelope<?> request, T payload) {
        long reqId = reqIdCounter.incrementAndGet();
        return new RpcEnvelope<>(request.recipient(), request.sender(), System.currentTimeMillis(), reqId, request.requestId(), payload);
    }

}
