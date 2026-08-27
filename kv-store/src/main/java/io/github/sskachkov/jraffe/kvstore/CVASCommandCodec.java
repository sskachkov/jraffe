package io.github.sskachkov.jraffe.kvstore;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class CVASCommandCodec {
    private CVASCommandCodec() {}

    public static byte[] encodeRequest(CVASRequest request) {
        byte[] key = request.key();
        byte[] fromV = request.fromValue();
        byte[] toV = request.toValue();
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 * Integer.BYTES + key.length + fromV.length + toV.length);
        buf.put(Opcodes.CVAS).putInt(key.length).putInt(fromV.length).put(key).put(fromV).put(toV);
        return buf.array();
    }

    public static CVASRequest decodeRequest(byte[] bytes) {
        if (bytes[0] != Opcodes.CVAS) {
            throw new IllegalArgumentException("expected opcode " + Opcodes.CVAS + ", got " + bytes[0]);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes, 1, bytes.length - 1);
        int keyLen = buf.getInt();
        int fromVLen = buf.getInt();
        byte[] key = new byte[keyLen];
        buf.get(key);
        byte[] fromV = new byte[fromVLen];
        buf.get(fromV);
        byte[] toV = new byte[buf.remaining()];
        buf.get(toV);
        return new CVASRequest(key, fromV, toV);
    }

    public static byte[] encodeResponse(CVASResponse response) {
        CVASResponse.Status status = response.status();
        byte statusCode = status.code;
        return switch (status) {
            case SUCCESS, KEY_NOT_FOUND -> {
                byte[] bytes = new byte[1];
                bytes[0] = statusCode;
                yield bytes;
            }
            case VALUE_MISMATCH -> {
                byte[] actualValue = response.actualValue();
                ByteBuffer buf = ByteBuffer.allocate(1 + actualValue.length);
                buf.put(statusCode).put(actualValue);
                yield buf.array();
            }
        };
    }

    public static CVASResponse decodeResponse(byte[] bytes) {
        CVASResponse.Status status = CVASResponse.Status.fromCode(bytes[0]);
        return switch (status) {
            case SUCCESS -> CVASResponse.success();
            case KEY_NOT_FOUND -> CVASResponse.keyNotFound();
            case VALUE_MISMATCH -> {
                byte[] actualV = Arrays.copyOfRange(bytes, 1, bytes.length);
                yield CVASResponse.valueMismatch(actualV);
            }
        };
    }
}
