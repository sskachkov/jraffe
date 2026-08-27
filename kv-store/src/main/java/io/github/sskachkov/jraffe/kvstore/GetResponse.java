package io.github.sskachkov.jraffe.kvstore;

public record GetResponse(boolean found, byte[] value) {}
