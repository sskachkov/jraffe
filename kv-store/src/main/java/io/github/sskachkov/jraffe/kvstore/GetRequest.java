package io.github.sskachkov.jraffe.kvstore;

public record GetRequest(byte[] key) implements Request {}
