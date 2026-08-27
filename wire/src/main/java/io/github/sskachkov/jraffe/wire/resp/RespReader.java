package io.github.sskachkov.jraffe.wire.resp;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespReader {
    private final PushbackInputStream in;

    public RespReader(InputStream in) {
        this.in = new PushbackInputStream(in);
    }

    public RespValue read() throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new IOException("connection closed while reading a RESP value");
        }
        String line = readLine();
        return switch (type) {
            case '+' -> new RespValue.SimpleString(line);
            case '-' -> new RespValue.RespError(line);
            case ':' -> new RespValue.RespInteger(Long.parseLong(line));
            case '$' -> readBulkString(Integer.parseInt(line));
            case '*' -> readArray(Integer.parseInt(line));
            default -> throw new IOException("unknown RESP type byte: " + (char) type);
        };
    }

    // Reads one client command, accepting either a properly framed RESP array (the format any real
    // client library sends) or a plain space-separated line (for humans typing directly into telnet,
    // same convenience real Redis offers). Both forms normalize to the same RespArray-of-BulkStrings shape.
    public RespValue.RespArray readCommand() throws IOException {
        int first = in.read();
        if (first == -1) {
            throw new IOException("connection closed while reading a command");
        }
        if (first == '*') {
            String line = readLine();
            RespValue value = readArray(Integer.parseInt(line));
            return (RespValue.RespArray) value;
        }
        in.unread(first);
        return readInlineCommand();
    }

    private RespValue.RespArray readInlineCommand() throws IOException {
        String line = readLine();
        List<RespValue> args = new ArrayList<>();
        for (String token : tokenize(line)) {
            args.add(new RespValue.BulkString(token.getBytes(StandardCharsets.UTF_8)));
        }
        return new RespValue.RespArray(args);
    }

    // splits on whitespace, treating a double-quoted section as a single token (quotes stripped,
    // spaces inside preserved) -- e.g. `set key "hello world"` -> ["set", "key", "hello world"]
    private static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (Character.isWhitespace(c) && !inQuotes) {
                if (!current.isEmpty()) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            tokens.add(current.toString());
        }
        return tokens;
    }

    private RespValue readBulkString(int length) throws IOException {
        if (length == -1) {
            return new RespValue.Nil();
        }
        byte[] data = readExactly(length);
        readLine(); // trailing CRLF after the data block -- required by the protocol, not part of the value
        return new RespValue.BulkString(data);
    }

    private RespValue readArray(int count) throws IOException {
        if (count == -1) {
            return new RespValue.Nil();
        }
        List<RespValue> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(read());
        }
        return new RespValue.RespArray(values);
    }

    private byte[] readExactly(int length) throws IOException {
        byte[] buf = new byte[length];
        int offset = 0;
        while (offset < length) {
            int n = in.read(buf, offset, length - offset);
            if (n == -1) {
                throw new IOException("connection closed after reading " + offset + " of " + length + " expected bytes");
            }
            offset += n;
        }
        return buf;
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1 && c != '\n') {
            if (c != '\r') {
                sb.append((char) c);
            }
        }
        if (c == -1) {
            throw new IOException("connection closed while reading a line");
        }
        return sb.toString();
    }
}
