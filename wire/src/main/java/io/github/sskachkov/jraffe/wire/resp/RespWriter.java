package io.github.sskachkov.jraffe.wire.resp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespWriter {
    private final OutputStream out;

    public RespWriter(OutputStream out) {
        this.out = out;
    }

    public void writeCommand(String... args) throws IOException {
        byte[][] encoded = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            encoded[i] = args[i].getBytes(StandardCharsets.UTF_8);
        }
        writeCommand(encoded);
    }

    public void writeCommand(byte[]... args) throws IOException {
        List<RespValue> values = new ArrayList<>(args.length);
        for (byte[] arg : args) {
            values.add(new RespValue.BulkString(arg));
        }
        write(new RespValue.RespArray(values));
    }

    public void write(RespValue value) throws IOException {
        writeValue(value);
        out.flush();
    }

    private void writeValue(RespValue value) throws IOException {
        switch (value) {
            case RespValue.SimpleString(String s) -> writeLine('+', s);
            case RespValue.RespError(String message) -> writeLine('-', message);
            case RespValue.RespInteger(long i) -> writeLine(':', Long.toString(i));
            case RespValue.Nil ignored -> out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
            case RespValue.BulkString(byte[] bytes) -> {
                out.write(('$' + Integer.toString(bytes.length) + "\r\n").getBytes(StandardCharsets.UTF_8));
                out.write(bytes);
                out.write('\r');
                out.write('\n');
            }
            case RespValue.RespArray(List<RespValue> values) -> {
                out.write(('*' + Integer.toString(values.size()) + "\r\n").getBytes(StandardCharsets.UTF_8));
                for (RespValue v : values) {
                    writeValue(v);
                }
            }
        }
    }

    private void writeLine(char prefix, String content) throws IOException {
        out.write((prefix + content + "\r\n").getBytes(StandardCharsets.UTF_8));
    }
}
