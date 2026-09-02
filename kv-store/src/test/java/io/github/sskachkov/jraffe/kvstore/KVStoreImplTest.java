package io.github.sskachkov.jraffe.kvstore;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class KVStoreImplTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // -- get/set --

    @Test
    void getOnMissingKeyReturnsNotFound() {
        KVStoreImpl store = new KVStoreImpl();
        GetResponse response = GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("missing")))));
        assertFalse(response.found());
    }

    @Test
    void setThenGetReturnsStoredValue() {
        KVStoreImpl store = new KVStoreImpl();
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("key1"), bytes("value1"))));

        GetResponse response = GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("key1")))));
        assertTrue(response.found());
        assertArrayEquals(bytes("value1"), response.value());
    }

    @Test
    void setOverwritesExistingValue() {
        KVStoreImpl store = new KVStoreImpl();
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("key1"), bytes("first"))));
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("key1"), bytes("second"))));

        GetResponse response = GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("key1")))));
        assertArrayEquals(bytes("second"), response.value());
    }

    @Test
    void distinctKeysDoNotInterfere() {
        KVStoreImpl store = new KVStoreImpl();
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("a"), bytes("valueA"))));
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("b"), bytes("valueB"))));

        assertArrayEquals(bytes("valueA"), GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("a"))))).value());
        assertArrayEquals(bytes("valueB"), GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("b"))))).value());
    }

    @Test
    void keysAreComparedByContentNotArrayIdentity() {
        // two separately-allocated byte[] with identical content must address the same entry --
        // the internal map is keyed on ByteBuffer.wrap(key), not on the array reference.
        KVStoreImpl store = new KVStoreImpl();
        store.apply(SetCommandCodec.encodeRequest(new SetRequest("key1".getBytes(StandardCharsets.UTF_8), bytes("value1"))));

        GetResponse response = GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest("key1".getBytes(StandardCharsets.UTF_8)))));
        assertTrue(response.found());
        assertArrayEquals(bytes("value1"), response.value());
    }

    @Test
    void emptyValueRoundTrips() {
        KVStoreImpl store = new KVStoreImpl();
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("key1"), new byte[0])));

        GetResponse response = GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("key1")))));
        assertTrue(response.found());
        assertArrayEquals(new byte[0], response.value());
    }

    // -- cvas --

    @Test
    void cvasOnMissingKeyReturnsKeyNotFound() {
        KVStoreImpl store = new KVStoreImpl();
        CVASResponse response = CVASCommandCodec.decodeResponse(
                store.apply(CVASCommandCodec.encodeRequest(new CVASRequest(bytes("missing"), bytes("from"), bytes("to")))));
        assertEquals(CVASResponse.Status.KEY_NOT_FOUND, response.status());
    }

    @Test
    void cvasWithWrongFromValueReturnsMismatchAndLeavesValueUnchanged() {
        KVStoreImpl store = new KVStoreImpl();
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("key1"), bytes("actual"))));

        CVASResponse response = CVASCommandCodec.decodeResponse(
                store.apply(CVASCommandCodec.encodeRequest(new CVASRequest(bytes("key1"), bytes("wrong"), bytes("new")))));
        assertEquals(CVASResponse.Status.VALUE_MISMATCH, response.status());
        assertArrayEquals(bytes("actual"), response.actualValue());

        GetResponse getResponse = GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("key1")))));
        assertArrayEquals(bytes("actual"), getResponse.value());
    }

    @Test
    void cvasWithMatchingFromValueSwapsAndReturnsSuccess() {
        KVStoreImpl store = new KVStoreImpl();
        store.apply(SetCommandCodec.encodeRequest(new SetRequest(bytes("key1"), bytes("old"))));

        CVASResponse response = CVASCommandCodec.decodeResponse(
                store.apply(CVASCommandCodec.encodeRequest(new CVASRequest(bytes("key1"), bytes("old"), bytes("new")))));
        assertEquals(CVASResponse.Status.SUCCESS, response.status());

        GetResponse getResponse = GetCommandCodec.decodeResponse(
                store.apply(GetCommandCodec.encodeRequest(new GetRequest(bytes("key1")))));
        assertArrayEquals(bytes("new"), getResponse.value());
    }

    // -- malformed/unknown commands --

    @Test
    void emptyCommandReturnsErrorResponse() {
        KVStoreImpl store = new KVStoreImpl();
        ErrorResponse response = ErrorResponseCodec.decodeResponse(store.apply(new byte[0]));
        assertTrue(response.message().contains(String.valueOf(Opcodes.ERROR)));
    }

    @Test
    void unrecognizedOpcodeReturnsErrorResponseNamingTheOpcode() {
        KVStoreImpl store = new KVStoreImpl();
        byte unknownOpcode = 99;
        ErrorResponse response = ErrorResponseCodec.decodeResponse(store.apply(new byte[]{unknownOpcode}));
        assertTrue(response.message().contains(String.valueOf(unknownOpcode)));
    }
}
