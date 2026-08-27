package io.github.sskachkov.jraffe.wire.resp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RespReaderTest {

    private static RespValue read(String encoded) throws IOException {
        var in = new ByteArrayInputStream(encoded.getBytes(StandardCharsets.UTF_8));
        return new RespReader(in).read();
    }

    @Test
    void simpleString() throws IOException {
        assertEquals(new RespValue.SimpleString("OK"), read("+OK\r\n"));
    }

    @Test
    void error() throws IOException {
        assertEquals(new RespValue.RespError("ERR something went wrong"), read("-ERR something went wrong\r\n"));
    }

    @Test
    void integer() throws IOException {
        assertEquals(new RespValue.RespInteger(1000), read(":1000\r\n"));
    }

    @Test
    void bulkString() throws IOException {
        RespValue value = read("$5\r\nhello\r\n");
        var bs = assertInstanceOf(RespValue.BulkString.class, value);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), bs.value());
    }

    @Test
    void bulkStringWithEmbeddedNewlineIsNotTruncated() throws IOException {
        RespValue value = read("$11\r\nhello\nworld\r\n");
        var bs = assertInstanceOf(RespValue.BulkString.class, value);
        assertEquals("hello\nworld", new String(bs.value(), StandardCharsets.UTF_8));
    }

    @Test
    void nilBulkString() throws IOException {
        assertEquals(new RespValue.Nil(), read("$-1\r\n"));
    }

    @Test
    void array() throws IOException {
        RespValue value = read("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        var arr = assertInstanceOf(RespValue.RespArray.class, value);
        List<RespValue> values = arr.values();
        assertEquals(2, values.size());
        assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), ((RespValue.BulkString) values.get(0)).value());
        assertArrayEquals("bar".getBytes(StandardCharsets.UTF_8), ((RespValue.BulkString) values.get(1)).value());
    }

    @Test
    void emptyArray() throws IOException {
        var arr = assertInstanceOf(RespValue.RespArray.class, read("*0\r\n"));
        assertEquals(0, arr.values().size());
    }

    @Test
    void nestedArray() throws IOException {
        RespValue value = read("*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n");
        var outer = assertInstanceOf(RespValue.RespArray.class, value);
        assertEquals(2, outer.values().size());
        var inner = assertInstanceOf(RespValue.RespArray.class, outer.values().get(0));
        assertEquals(1, inner.values().size());
    }

    @Test
    void connectionClosedMidValueThrows() {
        // declares 11 bytes but only provides 5 -- must not silently return a truncated value
        var in = new ByteArrayInputStream("$11\r\nhello".getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> new RespReader(in).read());
    }

    @Test
    void emptyStreamThrows() {
        var in = new ByteArrayInputStream(new byte[0]);
        assertThrows(IOException.class, () -> new RespReader(in).read());
    }

    @Test
    void unknownTypeByteThrows() {
        var in = new ByteArrayInputStream("!nope\r\n".getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> new RespReader(in).read());
    }

    private static RespValue.RespArray readCommand(String encoded) throws IOException {
        var in = new ByteArrayInputStream(encoded.getBytes(StandardCharsets.UTF_8));
        return new RespReader(in).readCommand();
    }

    private static List<String> argsOf(RespValue.RespArray array) {
        return array.values().stream()
                .map(v -> new String(((RespValue.BulkString) v).value(), StandardCharsets.UTF_8))
                .toList();
    }

    @Test
    void readCommandAcceptsFullyFramedRespArray() throws IOException {
        var array = readCommand("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        assertEquals(List.of("GET", "key"), argsOf(array));
    }

    @Test
    void readCommandAcceptsInlineText() throws IOException {
        var array = readCommand("STATUS\r\n");
        assertEquals(List.of("STATUS"), argsOf(array));
    }

    @Test
    void readCommandInlineSplitsOnWhitespace() throws IOException {
        var array = readCommand("SET key value\r\n");
        assertEquals(List.of("SET", "key", "value"), argsOf(array));
    }

    @Test
    void readCommandInlineRespectsDoubleQuotedSpaces() throws IOException {
        var array = readCommand("SET key \"hello world\"\r\n");
        assertEquals(List.of("SET", "key", "hello world"), argsOf(array));
    }

    @Test
    void readCommandInlineWorksWithBareLfToo() throws IOException {
        // some telnet clients/terminals send bare \n instead of \r\n
        var array = readCommand("STATUS\n");
        assertEquals(List.of("STATUS"), argsOf(array));
    }
}
