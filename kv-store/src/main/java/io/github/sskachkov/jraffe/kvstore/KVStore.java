package io.github.sskachkov.jraffe.kvstore;

public interface KVStore {
    byte [] apply(byte [] command);

}
