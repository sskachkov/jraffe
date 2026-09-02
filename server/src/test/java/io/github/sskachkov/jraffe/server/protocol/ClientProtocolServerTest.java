package io.github.sskachkov.jraffe.server.protocol;

import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.core.node.PeerReplicationStatus;
import io.github.sskachkov.jraffe.core.node.Role;
import io.github.sskachkov.jraffe.kvstore.CVASCommandCodec;
import io.github.sskachkov.jraffe.kvstore.CVASResponse;
import io.github.sskachkov.jraffe.kvstore.GetCommandCodec;
import io.github.sskachkov.jraffe.kvstore.GetResponse;
import io.github.sskachkov.jraffe.kvstore.SetCommandCodec;
import io.github.sskachkov.jraffe.kvstore.SetResponse;
import io.github.sskachkov.jraffe.server.FakeRaftNode;
import io.github.sskachkov.jraffe.wire.resp.RespValue;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ClientProtocolServerTest {

    private FakeRaftNode raftNode;
    private ClientProtocolServer server;

    @BeforeEach
    void setUp() throws IOException {
        raftNode = new FakeRaftNode();
        server = new ClientProtocolServer(raftNode, new SimpleMeterRegistry(), "n1", 0);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    private static RespValue.RespArray command(String... parts) {
        List<RespValue> values = new ArrayList<>();
        for (String part : parts) {
            values.add(new RespValue.BulkString(part.getBytes(StandardCharsets.UTF_8)));
        }
        return new RespValue.RespArray(values);
    }

    // -- dispatch-level --

    @Test
    void emptyRequestIsAnError() {
        RespValue reply = server.handleCommand(new RespValue.RespArray(List.of())).reply();
        assertInstanceOf(RespValue.RespError.class, reply);
    }

    @Test
    void unknownCommandNameIsAnError() {
        RespValue reply = server.handleCommand(command("NOPE")).reply();
        assertInstanceOf(RespValue.RespError.class, reply);
        assertTrue(((RespValue.RespError) reply).message().contains("NOPE"));
    }

    @Test
    void commandNamesAreCaseInsensitive() {
        raftNode.submitReadonlySyncResponse = RaftResponse.success(-1, "n1",
                GetCommandCodec.encodeResponse(new GetResponse(false, new byte[0])));
        RespValue reply = server.handleCommand(command("get", "key1")).reply();
        assertInstanceOf(RespValue.Nil.class, reply);
    }

    @Test
    void quitClosesTheConnectionAndRepliesOk() {
        var result = server.handleCommand(command("QUIT"));
        assertEquals(new RespValue.SimpleString("OK"), result.reply());
        assertTrue(result.close());
    }

    // -- SET --

    @Test
    void setReturnsOkOnSuccess() {
        raftNode.submitSyncResponse = RaftResponse.success(-1, "n1", SetCommandCodec.encodeResponse(new SetResponse()));
        RespValue reply = server.handleCommand(command("SET", "key1", "value1")).reply();
        assertEquals(new RespValue.SimpleString("OK"), reply);
    }

    @Test
    void setWithWrongArgCountIsAnError() {
        RespValue reply = server.handleCommand(command("SET", "key1")).reply();
        assertInstanceOf(RespValue.RespError.class, reply);
    }

    @Test
    void setReturnsNotLeaderErrorWithHint() {
        raftNode.submitSyncResponse = RaftResponse.notLeaderError(-1, "n1", "n2");
        RespValue reply = server.handleCommand(command("SET", "key1", "value1")).reply();
        assertInstanceOf(RespValue.RespError.class, reply);
        assertTrue(((RespValue.RespError) reply).message().contains("n2"));
    }

    @Test
    void setReturnsTimeoutError() {
        raftNode.submitSyncResponse = RaftResponse.timeoutError(-1, "n1");
        RespValue reply = server.handleCommand(command("SET", "key1", "value1")).reply();
        assertEquals(new RespValue.RespError("ERR timeout"), reply);
    }

    // -- GET --

    @Test
    void getReturnsValueWhenFound() {
        raftNode.submitReadonlySyncResponse = RaftResponse.success(-1, "n1",
                GetCommandCodec.encodeResponse(new GetResponse(true, "value1".getBytes(StandardCharsets.UTF_8))));
        RespValue reply = server.handleCommand(command("GET", "key1")).reply();
        assertInstanceOf(RespValue.BulkString.class, reply);
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), ((RespValue.BulkString) reply).value());
    }

    @Test
    void getReturnsNilWhenNotFound() {
        raftNode.submitReadonlySyncResponse = RaftResponse.success(-1, "n1",
                GetCommandCodec.encodeResponse(new GetResponse(false, new byte[0])));
        RespValue reply = server.handleCommand(command("GET", "key1")).reply();
        assertInstanceOf(RespValue.Nil.class, reply);
    }

    // -- CVAS --

    @Test
    void cvasReturnsOkOnSuccess() {
        raftNode.submitSyncResponse = RaftResponse.success(-1, "n1", CVASCommandCodec.encodeResponse(CVASResponse.success()));
        RespValue reply = server.handleCommand(command("CVAS", "key1", "old", "new")).reply();
        assertEquals(new RespValue.SimpleString("OK"), reply);
    }

    @Test
    void cvasReturnsErrorWhenKeyMissing() {
        raftNode.submitSyncResponse = RaftResponse.success(-1, "n1", CVASCommandCodec.encodeResponse(CVASResponse.keyNotFound()));
        RespValue reply = server.handleCommand(command("CVAS", "key1", "old", "new")).reply();
        assertInstanceOf(RespValue.RespError.class, reply);
    }

    @Test
    void cvasReturnsErrorWithActualValueOnMismatch() {
        raftNode.submitSyncResponse = RaftResponse.success(-1, "n1",
                CVASCommandCodec.encodeResponse(CVASResponse.valueMismatch("actual".getBytes(StandardCharsets.UTF_8))));
        RespValue reply = server.handleCommand(command("CVAS", "key1", "wrong", "new")).reply();
        assertInstanceOf(RespValue.RespError.class, reply);
        assertTrue(((RespValue.RespError) reply).message().contains("actual"));
    }

    // -- STATUS --

    @Test
    void statusReportsRoleTermAndNodeIdWhenFollower() {
        raftNode.role = Role.FOLLOWER;
        raftNode.currentTerm = 3;
        raftNode.id = "n1";

        RespValue.BulkString reply = (RespValue.BulkString) server.handleCommand(command("STATUS")).reply();
        String status = new String(reply.value(), StandardCharsets.UTF_8);
        assertTrue(status.contains("role=FOLLOWER"), status);
        assertTrue(status.contains("term=3"), status);
        assertTrue(status.contains("nodeId=n1"), status);
        assertFalse(status.contains("commitIndex"), status);
    }

    @Test
    void statusIncludesPeerReplicationDetailsWhenLeader() {
        raftNode.role = Role.LEADER;
        raftNode.commitIndex = 10;
        raftNode.peerIds = List.of("n2");
        raftNode.replicationStatus = Optional.of(Map.of("n2", new PeerReplicationStatus(5, 6, System.nanoTime())));

        RespValue.BulkString reply = (RespValue.BulkString) server.handleCommand(command("STATUS")).reply();
        String status = new String(reply.value(), StandardCharsets.UTF_8);
        assertTrue(status.contains("commitIndex=10"), status);
        assertTrue(status.contains("n2:matchIndex=5 nextIndex=6"), status);
    }

    // -- STATS --

    @Test
    void statsReflectsRegisteredMetrics() throws IOException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.counter("requests.total").increment();
        ClientProtocolServer statsServer = new ClientProtocolServer(new FakeRaftNode(), registry, "n1", 0);
        try {
            RespValue.BulkString reply = (RespValue.BulkString) statsServer.handleCommand(command("STATS")).reply();
            assertTrue(new String(reply.value(), StandardCharsets.UTF_8).contains("requests.total"));
        } finally {
            statsServer.shutdown();
        }
    }
}
