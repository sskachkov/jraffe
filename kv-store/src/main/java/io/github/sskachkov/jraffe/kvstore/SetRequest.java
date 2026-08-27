package io.github.sskachkov.jraffe.kvstore;

public record SetRequest(byte[] key, byte[] value) implements Request {}
