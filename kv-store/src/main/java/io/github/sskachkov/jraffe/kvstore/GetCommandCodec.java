package io.github.sskachkov.jraffe.kvstore;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class GetCommandCodec {
    private GetCommandCodec() {}

    public static byte[] encodeRequest(GetRequest request) {
        ByteBuffer buf = ByteBuffer.allocate(1 + request.key().length);
        buf.put(Opcodes.GET).put(request.key());
        return buf.array();
    }

    public static GetRequest decodeRequest(byte[] bytes) {
        if (bytes[0] != Opcodes.GET) {
            throw new IllegalArgumentException("expected opcode " + Opcodes.GET + ", got " + bytes[0]);
        }
        return new GetRequest(Arrays.copyOfRange(bytes, 1, bytes.length));
    }

    public static byte[] encodeResponse(GetResponse response) {
        ByteBuffer buf = ByteBuffer.allocate(1 + response.value().length);
        buf.put((byte) (response.found() ? 1 : 0)).put(response.value());
        return buf.array();
    }

    public static GetResponse decodeResponse(byte[] bytes) {
        boolean found = bytes[0] != 0;
        return new GetResponse(found, Arrays.copyOfRange(bytes, 1, bytes.length));
    }
}
