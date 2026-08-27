package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.kvstore.KVStore;
import io.github.sskachkov.jraffe.kvstore.KVStoreImpl;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class TestStateMachineAdapter implements StateMachine {
    private final Map<ByteBuffer, CompletableFuture<byte[]>> callbacks = new ConcurrentHashMap<>();
    private final KVStore kvStore;

    public TestStateMachineAdapter() {
        this.kvStore = new KVStoreImpl();
    }

    @Override
    public byte[] apply(byte[] command) {
        byte[] response = this.kvStore.apply(command);
        CompletableFuture<byte[]> future = callbacks.get(ByteBuffer.wrap(command));
        if (future != null)
            future.complete(response);
        return response;
    }

    public CompletableFuture<byte[]> registerCallback(byte[] command) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        callbacks.put(ByteBuffer.wrap(command), future);
        future.orTimeout(2, TimeUnit.SECONDS);
        return future;
    }
}
