package io.github.sskachkov.jraffe.kvstore;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SetCommandCodecTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void requestRoundTripsKeyAndValue() {
        SetRequest original = new SetRequest(bytes("key1"), bytes("value1"));
        SetRequest decoded = SetCommandCodec.decodeRequest(SetCommandCodec.encodeRequest(original));
        assertArrayEquals(original.key(), decoded.key());
        assertArrayEquals(original.value(), decoded.value());
    }

    @Test
    void requestRoundTripsEmptyKeyAndValue() {
        SetRequest original = new SetRequest(new byte[0], new byte[0]);
        SetRequest decoded = SetCommandCodec.decodeRequest(SetCommandCodec.encodeRequest(original));
        assertArrayEquals(new byte[0], decoded.key());
        assertArrayEquals(new byte[0], decoded.value());
    }

    @Test
    void decodeRequestRejectsWrongOpcode() {
        byte[] malformed = GetCommandCodec.encodeRequest(new GetRequest(bytes("k")));
        assertThrows(IllegalArgumentException.class, () -> SetCommandCodec.decodeRequest(malformed));
    }

    @Test
    void responseEncodesToEmptyBytes() {
        assertEquals(0, SetCommandCodec.encodeResponse(new SetResponse()).length);
    }

    @Test
    void responseDecodesFromEmptyBytes() {
        assertNotNull(SetCommandCodec.decodeResponse(new byte[0]));
    }
}
