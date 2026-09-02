package io.github.sskachkov.jraffe.kvstore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorResponseCodecTest {

    @Test
    void responseRoundTripsMessage() {
        ErrorResponse original = new ErrorResponse("something went wrong");
        ErrorResponse decoded = ErrorResponseCodec.decodeResponse(ErrorResponseCodec.encodeResponse(original));
        assertEquals(original.message(), decoded.message());
    }

    @Test
    void responseRoundTripsEmptyMessage() {
        ErrorResponse original = new ErrorResponse("");
        ErrorResponse decoded = ErrorResponseCodec.decodeResponse(ErrorResponseCodec.encodeResponse(original));
        assertEquals("", decoded.message());
    }

    @Test
    void responseRoundTripsNonAsciiMessage() {
        ErrorResponse original = new ErrorResponse("ошибка: ключ не найден — café");
        ErrorResponse decoded = ErrorResponseCodec.decodeResponse(ErrorResponseCodec.encodeResponse(original));
        assertEquals(original.message(), decoded.message());
    }
}
