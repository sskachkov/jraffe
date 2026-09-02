package io.github.sskachkov.jraffe.kvstore;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class GetCommandCodecTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void requestRoundTripsKey() {
        GetRequest original = new GetRequest(bytes("some-key"));
        GetRequest decoded = GetCommandCodec.decodeRequest(GetCommandCodec.encodeRequest(original));
        assertArrayEquals(original.key(), decoded.key());
    }

    @Test
    void requestRoundTripsEmptyKey() {
        GetRequest original = new GetRequest(new byte[0]);
        GetRequest decoded = GetCommandCodec.decodeRequest(GetCommandCodec.encodeRequest(original));
        assertArrayEquals(new byte[0], decoded.key());
    }

    @Test
    void decodeRequestRejectsWrongOpcode() {
        byte[] malformed = SetCommandCodec.encodeRequest(new SetRequest(bytes("k"), bytes("v")));
        assertThrows(IllegalArgumentException.class, () -> GetCommandCodec.decodeRequest(malformed));
    }

    @Test
    void responseRoundTripsFoundWithValue() {
        GetResponse original = new GetResponse(true, bytes("value1"));
        GetResponse decoded = GetCommandCodec.decodeResponse(GetCommandCodec.encodeResponse(original));
        assertTrue(decoded.found());
        assertArrayEquals(bytes("value1"), decoded.value());
    }

    @Test
    void responseRoundTripsNotFound() {
        GetResponse original = new GetResponse(false, new byte[0]);
        GetResponse decoded = GetCommandCodec.decodeResponse(GetCommandCodec.encodeResponse(original));
        assertFalse(decoded.found());
        assertArrayEquals(new byte[0], decoded.value());
    }
}
