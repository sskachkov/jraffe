package io.github.sskachkov.jraffe.kvstore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class ErrorResponseCodec {
    private ErrorResponseCodec() {}

    public static byte[] encodeResponse(ErrorResponse response) {
        byte[] messageBytes = response.message().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + messageBytes.length);
        buf.put(Opcodes.ERROR).put(messageBytes);
        return buf.array();
    }

    public static ErrorResponse decodeResponse(byte[] bytes) {
        return new ErrorResponse(new String(bytes, 1, bytes.length - 1, StandardCharsets.UTF_8));
    }
}
