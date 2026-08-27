package io.github.sskachkov.jraffe.client;

import io.github.sskachkov.jraffe.wire.resp.RespReader;
import io.github.sskachkov.jraffe.wire.resp.RespValue;
import io.github.sskachkov.jraffe.wire.resp.RespWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class RespClientTest {

    // a minimal fake server: accepts one connection, echoes the command name back as a bulk string reply
    private static ServerSocket startFakeServer() throws IOException {
        ServerSocket server = new ServerSocket(0);
        Thread thread = new Thread(() -> {
            try (Socket socket = server.accept()) {
                RespValue request = new RespReader(socket.getInputStream()).read();
                var args = ((RespValue.RespArray) request).values();
                var commandName = ((RespValue.BulkString) args.get(0)).value();
                new RespWriter(socket.getOutputStream()).write(new RespValue.BulkString(commandName));
            } catch (IOException ignored) {
                // test socket closed after assertions -- nothing to do
            }
        });
        thread.setDaemon(true);
        thread.start();
        return server;
    }

    @Test
    void sendCommandReturnsParsedReply() throws IOException {
        ServerSocket server = startFakeServer();
        try (RespClient client = new RespClient("127.0.0.1", server.getLocalPort())) {
            RespValue reply = client.sendCommand("STATUS");
            var bulkString = assertInstanceOf(RespValue.BulkString.class, reply);
            assertEquals("STATUS", new String(bulkString.value(), StandardCharsets.UTF_8));
        } finally {
            server.close();
        }
    }

    @Test
    void multiArgCommandIsSentAsAnArray() throws IOException {
        ServerSocket server = new ServerSocket(0);
        try {
            Thread thread = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    RespValue request = new RespReader(socket.getInputStream()).read();
                    var array = assertInstanceOf(RespValue.RespArray.class, request);
                    List<String> args = array.values().stream()
                            .map(v -> new String(((RespValue.BulkString) v).value(), StandardCharsets.UTF_8))
                            .toList();
                    new RespWriter(socket.getOutputStream()).write(new RespValue.SimpleString(String.join(",", args)));
                } catch (IOException ignored) {
                }
            });
            thread.setDaemon(true);
            thread.start();

            try (RespClient client = new RespClient("127.0.0.1", server.getLocalPort())) {
                RespValue reply = client.sendCommand("SET", "key", "value");
                assertEquals(new RespValue.SimpleString("SET,key,value"), reply);
            }
        } finally {
            server.close();
        }
    }
}
