package io.github.sskachkov.jraffe.server.statemachine;

import io.github.sskachkov.jraffe.kvstore.KVStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class KvStoreStateMachineTest {

    @Test
    void applyDelegatesToTheUnderlyingStoreAndReturnsItsResult() {
        byte[] command = {1, 2, 3};
        byte[] expected = {9, 8, 7};
        KVStore store = cmd -> {
            assertSame(command, cmd);
            return expected;
        };

        KvStoreStateMachine stateMachine = new KvStoreStateMachine(store);
        assertArrayEquals(expected, stateMachine.apply(command));
    }
}
