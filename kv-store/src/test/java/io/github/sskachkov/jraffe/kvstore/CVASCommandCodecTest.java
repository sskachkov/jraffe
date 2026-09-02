package io.github.sskachkov.jraffe.kvstore;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class CVASCommandCodecTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void requestRoundTripsKeyFromAndToValues() {
        CVASRequest original = new CVASRequest(bytes("key1"), bytes("from"), bytes("to"));
        CVASRequest decoded = CVASCommandCodec.decodeRequest(CVASCommandCodec.encodeRequest(original));
        assertArrayEquals(original.key(), decoded.key());
        assertArrayEquals(original.fromValue(), decoded.fromValue());
        assertArrayEquals(original.toValue(), decoded.toValue());
    }

    @Test
    void requestRoundTripsEmptyKeyAndValues() {
        CVASRequest original = new CVASRequest(new byte[0], new byte[0], new byte[0]);
        CVASRequest decoded = CVASCommandCodec.decodeRequest(CVASCommandCodec.encodeRequest(original));
        assertArrayEquals(new byte[0], decoded.key());
        assertArrayEquals(new byte[0], decoded.fromValue());
        assertArrayEquals(new byte[0], decoded.toValue());
    }

    @Test
    void decodeRequestRejectsWrongOpcode() {
        byte[] malformed = GetCommandCodec.encodeRequest(new GetRequest(bytes("k")));
        assertThrows(IllegalArgumentException.class, () -> CVASCommandCodec.decodeRequest(malformed));
    }

    @Test
    void responseRoundTripsSuccess() {
        CVASResponse decoded = CVASCommandCodec.decodeResponse(CVASCommandCodec.encodeResponse(CVASResponse.success()));
        assertEquals(CVASResponse.Status.SUCCESS, decoded.status());
    }

    @Test
    void responseRoundTripsKeyNotFound() {
        CVASResponse decoded = CVASCommandCodec.decodeResponse(CVASCommandCodec.encodeResponse(CVASResponse.keyNotFound()));
        assertEquals(CVASResponse.Status.KEY_NOT_FOUND, decoded.status());
    }

    @Test
    void responseRoundTripsValueMismatchWithActualValue() {
        CVASResponse original = CVASResponse.valueMismatch(bytes("actual"));
        CVASResponse decoded = CVASCommandCodec.decodeResponse(CVASCommandCodec.encodeResponse(original));
        assertEquals(CVASResponse.Status.VALUE_MISMATCH, decoded.status());
        assertArrayEquals(bytes("actual"), decoded.actualValue());
    }

    @Test
    void decodeResponseRejectsUnknownStatusCode() {
        byte[] malformedResponse = {(byte) 42};
        assertThrows(IllegalArgumentException.class, () -> CVASCommandCodec.decodeResponse(malformedResponse));
    }
}
