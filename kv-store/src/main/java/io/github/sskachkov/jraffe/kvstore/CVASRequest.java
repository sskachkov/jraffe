package io.github.sskachkov.jraffe.kvstore;

public record CVASRequest(byte[] key, byte [] fromValue, byte[] toValue) implements Request {}
