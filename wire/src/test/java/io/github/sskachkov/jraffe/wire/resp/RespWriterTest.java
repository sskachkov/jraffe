package io.github.sskachkov.jraffe.wire.resp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RespWriterTest {

    private static String writeCommand(String... args) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new RespWriter(out).writeCommand(args);
        return out.toString(StandardCharsets.UTF_8);
    }

    private static String writeValue(RespValue value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new RespWriter(out).write(value);
        return out.toString(StandardCharsets.UTF_8);
    }

    @Test
    void singleArgCommand() throws IOException {
        assertEquals("*1\r\n$6\r\nSTATUS\r\n", writeCommand("STATUS"));
    }

    @Test
    void multiArgCommand() throws IOException {
        assertEquals("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", writeCommand("SET", "key", "value"));
    }

    @Test
    void valueWithEmbeddedNewlineIsLengthPrefixedNotEscaped() throws IOException {
        assertEquals("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello\nworld\r\n", writeCommand("SET", "key", "hello\nworld"));
    }

    @Test
    void lengthIsByteLengthNotCharLength() throws IOException {
        // 'é' is 1 char but 2 bytes in UTF-8 -- the declared length must reflect bytes, not String.length()
        assertEquals("*1\r\n$5\r\ncafé\r\n", writeCommand("café"));
    }

    @Test
    void zeroArgCommand() throws IOException {
        assertEquals("*0\r\n", writeCommand());
    }

    @Test
    void simpleStringReply() throws IOException {
        assertEquals("+OK\r\n", writeValue(new RespValue.SimpleString("OK")));
    }

    @Test
    void errorReply() throws IOException {
        assertEquals("-ERR bad command\r\n", writeValue(new RespValue.RespError("ERR bad command")));
    }

    @Test
    void integerReply() throws IOException {
        assertEquals(":42\r\n", writeValue(new RespValue.RespInteger(42)));
    }

    @Test
    void nilReply() throws IOException {
        assertEquals("$-1\r\n", writeValue(new RespValue.Nil()));
    }

    @Test
    void arrayReplyOfMixedTypes() throws IOException {
        var array = new RespValue.RespArray(List.of(
                new RespValue.SimpleString("OK"),
                new RespValue.BulkString("hi".getBytes(StandardCharsets.UTF_8)),
                new RespValue.Nil()
        ));
        assertEquals("*3\r\n+OK\r\n$2\r\nhi\r\n$-1\r\n", writeValue(array));
    }
}
