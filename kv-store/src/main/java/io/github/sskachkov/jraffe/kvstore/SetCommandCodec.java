package io.github.sskachkov.jraffe.kvstore;

import java.nio.ByteBuffer;

public final class SetCommandCodec {
    private SetCommandCodec() {}

    public static byte[] encodeRequest(SetRequest request) {
        byte[] key = request.key();
        byte[] value = request.value();
        ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES + key.length + value.length);
        buf.put(Opcodes.SET).putInt(key.length).put(key).put(value);
        return buf.array();
    }

    public static SetRequest decodeRequest(byte[] bytes) {
        if (bytes[0] != Opcodes.SET) {
            throw new IllegalArgumentException("expected opcode " + Opcodes.SET + ", got " + bytes[0]);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes, 1, bytes.length - 1);
        int keyLen = buf.getInt();
        byte[] key = new byte[keyLen];
        buf.get(key);
        byte[] value = new byte[buf.remaining()];
        buf.get(value);
        return new SetRequest(key, value);
    }

    public static byte[] encodeResponse(SetResponse response) {
        return new byte[0];
    }

    public static SetResponse decodeResponse(byte[] bytes) {
        return new SetResponse();
    }
}
