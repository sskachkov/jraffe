package io.github.sskachkov.jraffe.wire.resp;

import java.util.List;

public sealed interface RespValue {
    record SimpleString(String value) implements RespValue {}
    record RespError(String message) implements RespValue {}
    record RespInteger(long value) implements RespValue {}
    record BulkString(byte[] value) implements RespValue {}
    record Nil() implements RespValue {}
    record RespArray(List<RespValue> values) implements RespValue {}
}
