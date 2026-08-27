package io.github.sskachkov.jraffe.server.statemachine;

import io.github.sskachkov.jraffe.core.StateMachine;
import io.github.sskachkov.jraffe.kvstore.KVStore;

public class KvStoreStateMachine implements StateMachine {
    private final KVStore kvStore;

    public KvStoreStateMachine(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public byte[] apply(byte[] command) {
        return kvStore.apply(command);
    }
}
