package io.github.sskachkov.jraffe.wire.resp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class RespRoundTripTest {

    // a command is just a RESP array of bulk strings -- the reader can parse the writer's own output directly
    @Test
    void writerOutputParsesBackToTheSameArgs() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new RespWriter(out).writeCommand("SET", "key", "hello\nworld");

        var in = new ByteArrayInputStream(out.toByteArray());
        RespValue value = new RespReader(in).read();

        var arr = assertInstanceOf(RespValue.RespArray.class, value);
        assertEquals(3, arr.values().size());
        assertEquals("SET", asString(arr.values().get(0)));
        assertEquals("key", asString(arr.values().get(1)));
        assertEquals("hello\nworld", asString(arr.values().get(2)));
    }

    @Test
    void genericWriteRoundTripsAnyValueType() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespValue original = new RespValue.RespArray(java.util.List.of(
                new RespValue.SimpleString("OK"),
                new RespValue.RespError("ERR nope"),
                new RespValue.RespInteger(7),
                new RespValue.Nil(),
                new RespValue.BulkString("hi".getBytes(StandardCharsets.UTF_8))
        ));
        new RespWriter(out).write(original);

        var in = new ByteArrayInputStream(out.toByteArray());
        var parsed = assertInstanceOf(RespValue.RespArray.class, new RespReader(in).read());

        assertEquals(5, parsed.values().size());
        assertEquals(new RespValue.SimpleString("OK"), parsed.values().get(0));
        assertEquals(new RespValue.RespError("ERR nope"), parsed.values().get(1));
        assertEquals(new RespValue.RespInteger(7), parsed.values().get(2));
        assertEquals(new RespValue.Nil(), parsed.values().get(3));
        assertEquals("hi", asString(parsed.values().get(4)));
    }

    private static String asString(RespValue value) {
        return new String(((RespValue.BulkString) value).value(), StandardCharsets.UTF_8);
    }
}
