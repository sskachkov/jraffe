package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
import io.github.sskachkov.jraffe.kvstore.GetCommandCodec;
import io.github.sskachkov.jraffe.kvstore.GetRequest;
import io.github.sskachkov.jraffe.kvstore.GetResponse;
import io.github.sskachkov.jraffe.kvstore.SetCommandCodec;
import io.github.sskachkov.jraffe.kvstore.SetRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class StaleLeaderReadTest {
    private static final List<String> NODE_IDS = List.of("n1", "n2", "n3");
    private InMemoryCluster cluster;
    private List<RaftNode> nodes;

    @BeforeEach
    void setUp() {
        cluster = new InMemoryCluster();
        nodes = cluster.start(NODE_IDS);
        cluster.isolate("n1"); // exists only as a phantom peer, never actually reachable
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::shutdown);
    }

    @Test
    void newLeaderMustNotServeStaleReadForAlreadyReplicatedButUnappliedEntry() throws Exception {
        RaftNode n2 = nodes.get(1);
        RaftNode n3 = nodes.get(2);

        // Simulate a write a previous (now-gone) leader already replicated to n2 and n3 in
        // term 1, but neither learned was committed (no follow-up heartbeat carrying the
        // updated leaderCommit arrived) before that leader disappeared.
        byte[] setCmd = SetCommandCodec.encodeRequest(new SetRequest("k".getBytes(), "v".getBytes()));
        LogEntry entry = new LogEntry(1, 1, setCmd, false);
        var req = new RaftRpcClient.AppendEntriesRequest(1, 1, "ghost-leader", 0, 0, List.of(entry), /*leaderCommit=*/0);
        n2.handleAppendEntries(req);
        n3.handleAppendEntries(req);

        RaftNode newLeader = RaftTestUtils.awaitLeader(List.of(n2, n3), Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected"));

        byte[] getCmd = GetCommandCodec.encodeRequest(new GetRequest("k".getBytes()));
        RaftResponse rr = newLeader.submitReadonly(new RaftMessage(getCmd)).get(5, TimeUnit.SECONDS);

        assertTrue(rr.isSuccess(), "read failed: " + (rr.isSuccess() ? "" : rr.getError()));
        GetResponse getResponse = GetCommandCodec.decodeResponse(rr.getData());
        assertTrue(getResponse.found(),
                "new leader served a stale read: missed an already-committed write from before it took office");
        assertEquals("v", new String(getResponse.value()));
    }
}
