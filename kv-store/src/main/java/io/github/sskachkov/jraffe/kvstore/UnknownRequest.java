package io.github.sskachkov.jraffe.kvstore;

public record UnknownRequest(byte opcode) implements Request {}
