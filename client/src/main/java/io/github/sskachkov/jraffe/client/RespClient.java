package io.github.sskachkov.jraffe.client;

import io.github.sskachkov.jraffe.wire.resp.RespReader;
import io.github.sskachkov.jraffe.wire.resp.RespValue;
import io.github.sskachkov.jraffe.wire.resp.RespWriter;

import java.io.IOException;
import java.net.Socket;

public class RespClient implements AutoCloseable {
    private final Socket socket;
    private final RespReader reader;
    private final RespWriter writer;

    public RespClient(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.reader = new RespReader(socket.getInputStream());
        this.writer = new RespWriter(socket.getOutputStream());
    }

    public RespValue sendCommand(String... args) throws IOException {
        writer.writeCommand(args);
        return reader.read();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
