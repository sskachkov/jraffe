package io.github.sskachkov.jraffe.server.protocol;

import io.github.sskachkov.jraffe.core.error.NotLeaderError;
import io.github.sskachkov.jraffe.core.error.RaftError;
import io.github.sskachkov.jraffe.core.error.TimeoutError;
import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.core.node.PeerReplicationStatus;
import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.node.Role;
import io.github.sskachkov.jraffe.kvstore.*;
import io.github.sskachkov.jraffe.server.metrics.MetricsFormatter;
import io.github.sskachkov.jraffe.wire.resp.RespReader;
import io.github.sskachkov.jraffe.wire.resp.RespValue;
import io.github.sskachkov.jraffe.wire.resp.RespWriter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ClientProtocolServer {

    private final Logger log;
    private final RaftNode raftNode;
    private final MeterRegistry registry;
    private final ServerSocket serverSocket;
    private volatile boolean running = true;

    public ClientProtocolServer(RaftNode raftNode, MeterRegistry registry, String nodeId, int port) throws IOException {
        this.log = new ContextAwareLogger(ClientProtocolServer.class, nodeId);
        this.raftNode = raftNode;
        this.registry = registry;
        this.serverSocket = new ServerSocket(port);
    }

    public void start() {
        Thread.ofVirtual().start(this::acceptLoop);
    }

    private static RespValue convertRaftError(RaftError error) {
        if (error instanceof NotLeaderError nle) {
            String hint = nle.getSuggestedLeader() != null ? " leader is " + nle.getSuggestedLeader() : "";
            return new RespValue.RespError("ERR not leader" + hint);
        }
        if (error instanceof TimeoutError) {
            return new RespValue.RespError("ERR timeout");
        }
        return new RespValue.RespError("ERR internal error");

    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                Thread.ofVirtual().start(() -> handleConnection(socket));
            } catch (IOException e) {
                if (running) {
                    log.error("Error accepting client connection", e);
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket) {
            var reader = new RespReader(socket.getInputStream());
            var writer = new RespWriter(socket.getOutputStream());
            while (true) {
                RespValue.RespArray request = reader.readCommand();
                CommandResult result = handleCommand(request);
                writer.write(result.reply());
                if (result.close()) {
                    break;
                }
            }
        } catch (IOException e) {
            log.debug("Client connection closed: {}", e.getMessage());
        }
    }

    CommandResult handleCommand(RespValue.RespArray request) {
        if (request.values().isEmpty()) {
            return CommandResult.of(new RespValue.RespError("ERR invalid command"));
        }
        if (!(request.values().get(0) instanceof RespValue.BulkString nameValue)) {
            return CommandResult.of(new RespValue.RespError("ERR invalid command name"));
        }
        String commandName = new String(nameValue.value(), StandardCharsets.UTF_8).toUpperCase();
        return switch (commandName) {
            case "STATUS" -> CommandResult.of(handleStatus());
            case "STATS" -> CommandResult.of(handleStats());
            case "SET" -> CommandResult.of(handleSet(request));
            case "GET" -> CommandResult.of(handleGet(request));
            case "CVAS" -> CommandResult.of(handleCvas(request));
            case "QUIT" -> new CommandResult(new RespValue.SimpleString("OK"), true);
            default -> CommandResult.of(new RespValue.RespError("ERR unknown command '" + commandName + "'"));
        };
    }

    private RespValue handleStatus() {
        StringBuilder status = new StringBuilder();
        status.append("role=").append(raftNode.getRole()).append(" term=").append(raftNode.getCurrentTerm()).
                append(" nodeId=").append(raftNode.getId());
        if (raftNode.getRole() == Role.LEADER) {
            raftNode.getReplicationStatus().ifPresent(statuses -> { //empty on the rare race where leadership just changed
                status.append(" commitIndex=").append(raftNode.getCommitIndex());
                for (String peerId : raftNode.getPeerIds()) {
                    PeerReplicationStatus progress = statuses.get(peerId);
                    status.append("\n").append(peerId).append(":").append("matchIndex=").append(progress.matchIndex())
                            .append(" nextIndex=").append(progress.nextIndex());
                    long currentTime = System.nanoTime();
                    status.append(" lastConfirmedDispatchMs=").append((currentTime - progress.lastConfirmedDispatchAt()) / 1000000);
                }
            });
        }
        return new RespValue.BulkString(status.toString().getBytes(StandardCharsets.UTF_8));
    }

    private RespValue handleSet(RespValue.RespArray request) {
        if (request.values().size() != 3
                || !(request.values().get(1) instanceof RespValue.BulkString keyValue)
                || !(request.values().get(2) instanceof RespValue.BulkString valueValue)) {
            return new RespValue.RespError("ERR wrong number of arguments for 'SET'");
        }
        byte[] command = SetCommandCodec.encodeRequest(new SetRequest(keyValue.value(), valueValue.value()));
        RaftResponse rr = raftNode.submitSync(new RaftMessage(command));
        if (rr.isSuccess()) {
            return new RespValue.SimpleString("OK");
        } else {
            return convertRaftError(rr.getError());
        }
    }

    private RespValue handleGet(RespValue.RespArray request) {
        if (request.values().size() != 2 || !(request.values().get(1) instanceof RespValue.BulkString keyValue)) {
            return new RespValue.RespError("ERR wrong number of arguments for 'GET'");
        }
        byte[] command = GetCommandCodec.encodeRequest(new GetRequest(keyValue.value()));
        RaftResponse rr = raftNode.submitReadonlySync(new RaftMessage(command));
        if (rr.isSuccess()) {
            GetResponse response = GetCommandCodec.decodeResponse(rr.getData());
            return response.found() ? new RespValue.BulkString(response.value()) : new RespValue.Nil();
        } else {
            return convertRaftError(rr.getError());
        }
    }

    private RespValue handleCvas(RespValue.RespArray request) {
        if (request.values().size() != 4
                || !(request.values().get(1) instanceof RespValue.BulkString keyValue)
                || !(request.values().get(2) instanceof RespValue.BulkString fromValue)
                || !(request.values().get(3) instanceof RespValue.BulkString toValue)) {
            return new RespValue.RespError("ERR wrong number of arguments for 'CVAS'");
        }
        byte[] command = CVASCommandCodec.encodeRequest(new CVASRequest(keyValue.value(), fromValue.value(), toValue.value()));
        RaftResponse rr = raftNode.submitSync(new RaftMessage(command));
        if (!rr.isSuccess()) {
            return convertRaftError(rr.getError());
        }
        CVASResponse response = CVASCommandCodec.decodeResponse(rr.getData());
        return switch (response.status()) {
            case SUCCESS -> new RespValue.SimpleString("OK");
            case KEY_NOT_FOUND -> new RespValue.RespError("ERR key does not exist");
            case VALUE_MISMATCH -> new RespValue.RespError("ERR value mismatch, actual value: "
                    + new String(response.actualValue(), StandardCharsets.UTF_8));
        };
    }

    private RespValue handleStats() {
        String stats = String.join("\n", MetricsFormatter.format(registry));
        return new RespValue.BulkString(stats.getBytes(StandardCharsets.UTF_8));
    }

    public void shutdown() throws IOException {
        running = false;
        serverSocket.close();
    }

    record CommandResult(RespValue reply, boolean close) {
        static CommandResult of(RespValue reply) {
            return new CommandResult(reply, false);
        }
    }
}
