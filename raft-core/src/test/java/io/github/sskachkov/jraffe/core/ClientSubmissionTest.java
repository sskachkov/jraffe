package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.kvstore.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class ClientSubmissionTest {
    private static final List<String> NODE_IDS = List.of("n1", "n2", "n3", "n4", "n5");
    private InMemoryCluster cluster;
    private List<RaftNode> nodes;

    private static <REQ, RESP> CompletableFuture<RESP> send(RaftNode node, REQ request, boolean readOnly,
                                             Function<REQ, byte[]> encoder,
                                             Function<byte[], RESP> decoder) throws InterruptedException {
        byte[] bytes = encoder.apply(request);
        CompletableFuture<RaftResponse> future;
        if (readOnly) {
            future = node.submitReadonly(new RaftMessage(bytes));
        } else {
            future = node.submit(new RaftMessage(bytes));
        }
        return future.thenCompose(rr -> {
            if (!rr.isSuccess()) {
                return CompletableFuture.failedFuture(new RuntimeException(rr.getError().getMessage()));
            }
            return CompletableFuture.completedFuture(decoder.apply(rr.getData()));
        });

    }

    private static CompletableFuture<SetResponse> sendSetRequest(RaftNode node, byte[] key, byte[] value) throws InterruptedException {
        return send(node, new SetRequest(key, value), false, SetCommandCodec::encodeRequest, SetCommandCodec::decodeResponse);
    }

    private static CompletableFuture<GetResponse> sendGetRequest(RaftNode node, byte[] key) throws InterruptedException {
        return send(node, new GetRequest(key), true, GetCommandCodec::encodeRequest, GetCommandCodec::decodeResponse);
    }

    private static CompletableFuture<CVASResponse> sendCVASRequest(RaftNode node, byte[] key, byte[] fromValue, byte[] toValue) throws InterruptedException {
        return send(node, new CVASRequest(key, fromValue, toValue), false, CVASCommandCodec::encodeRequest, CVASCommandCodec::decodeResponse);
    }

    @BeforeEach
    void setUp() {
        cluster = new InMemoryCluster();
        nodes = cluster.start(NODE_IDS);
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::shutdown);
    }

    @Test
    public void getNotFound() throws InterruptedException, ExecutionException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        String key = "key1";
        GetResponse getResponse = sendGetRequest(leader, key.getBytes()).get();
        assertFalse(getResponse.found(), "Leader responded with value.");
    }

    @Test
    public void simpleSetAndGet() throws InterruptedException, ExecutionException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        String key = "key1";
        String originalValue = "value1";
        //set
        SetResponse setResponse = sendSetRequest(leader, key.getBytes(), originalValue.getBytes()).get();

        //get
        GetResponse getResponse = sendGetRequest(leader, key.getBytes()).get();
        assertTrue(getResponse.found(), "Leader responded with not found.");
        assertEquals(originalValue, new String(getResponse.value(), StandardCharsets.UTF_8), "Received value is different from original.");
    }

    @Test
    public void simpleSetAndCvas() throws InterruptedException, ExecutionException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        String key = "key1";
        String originalValue = "value1";
        String newValue = "updated value";
        //set
        SetResponse setResponse = sendSetRequest(leader, key.getBytes(), originalValue.getBytes()).get();

        //cvas
        CVASResponse cvasResponse = sendCVASRequest(leader, key.getBytes(), originalValue.getBytes(), newValue.getBytes()).get();
        assertEquals(CVASResponse.Status.SUCCESS, cvasResponse.status(), "CVAS operation failed with status " + cvasResponse.status());

        //get
        GetResponse getResponse = sendGetRequest(leader, key.getBytes()).get();
        assertTrue(getResponse.found(), "Leader responded with not found.");
        assertEquals(newValue, new String(getResponse.value(), StandardCharsets.UTF_8), "Received value is different from expected.");

    }

    @Test
    public void cvasKeyNotFound() throws InterruptedException, ExecutionException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        String key = "key1";

        //cvas on a key that was never set
        CVASResponse cvasResponse = sendCVASRequest(leader, key.getBytes(), "old".getBytes(), "new".getBytes()).get();
        assertEquals(CVASResponse.Status.KEY_NOT_FOUND, cvasResponse.status());
    }

    @Test
    public void cvasValueMismatch() throws InterruptedException, ExecutionException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        String key = "key1";
        String originalValue = "value1";
        //set
        sendSetRequest(leader, key.getBytes(), originalValue.getBytes()).get();

        //cvas with a fromValue that doesn't match what's stored
        CVASResponse cvasResponse = sendCVASRequest(leader, key.getBytes(), "wrongExpected".getBytes(), "newValue".getBytes()).get();
        assertEquals(CVASResponse.Status.VALUE_MISMATCH, cvasResponse.status());
        assertEquals(originalValue, new String(cvasResponse.actualValue(), StandardCharsets.UTF_8),
                "actualValue should reflect what's actually stored.");

        //get - value should be unchanged since the swap did not happen
        GetResponse getResponse = sendGetRequest(leader, key.getBytes()).get();
        assertTrue(getResponse.found());
        assertEquals(originalValue, new String(getResponse.value(), StandardCharsets.UTF_8),
                "value should not have changed after a mismatch.");
    }

    @Test
    public void testSetApplyOnIsolation() throws InterruptedException, ExecutionException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        String leaderId = leader.getNodeId();
        TestStateMachineAdapter leaderStateMachine = this.cluster.stateMachineFor(leaderId);
        Iterator<String> pidsIterator = leader.getPeerIds().iterator();
        String peerId1 = pidsIterator.next();
        String peerId2 = pidsIterator.next();
        String peerId3 = pidsIterator.next();
        this.cluster.isolate(peerId1);
        this.cluster.isolate(peerId2);
        this.cluster.isolate(peerId3);

        String key = "key1";
        String originalValue = "value1";

        byte[] setReqBytes = SetCommandCodec.encodeRequest(new SetRequest(key.getBytes(), originalValue.getBytes()));
        CompletableFuture<byte[]> stateMachineFuture = leaderStateMachine.registerCallback(setReqBytes);
        long [] stateMachineApplyTime = new long[1];
        stateMachineFuture.thenAccept(bytes -> stateMachineApplyTime[0] = System.nanoTime());

        long [] cmdCompletionTime = new long[1];
        CompletableFuture<RaftResponse> setFuture = leader.submit(new RaftMessage(setReqBytes));
        setFuture.thenAccept(bytes -> cmdCompletionTime[0] = System.nanoTime());
        Thread.sleep(400);
        assertFalse(setFuture.isDone());

        this.cluster.heal(peerId1);
        this.cluster.heal(peerId2);
        this.cluster.heal(peerId3);
        RaftResponse rrSet = setFuture.get();
        assertTrue(rrSet.isSuccess());
        byte[] setResponseBytes = rrSet.getData();
        byte[] stateMachineResponseBytes = stateMachineFuture.get();
        assertNotEquals(0, stateMachineApplyTime[0]);
        assertNotEquals(0, cmdCompletionTime[0]);
        assertTrue(stateMachineApplyTime[0] <= cmdCompletionTime[0]);

        SetResponse setResponse = SetCommandCodec.decodeResponse(setResponseBytes);

    }

    @Test
    public void submitReadonlyBlocksUntilMajorityConfirmed() throws InterruptedException, ExecutionException, TimeoutException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));

        String key = "key1";
        String value = "value1";
        SetResponse setResponse = sendSetRequest(leader, key.getBytes(), value.getBytes()).get();

        for (String peerId : leader.getPeerIds()) {
            cluster.isolate(peerId);
        }

        CompletableFuture<GetResponse> future = sendGetRequest(leader, key.getBytes());
        Thread.sleep(400);
        assertFalse(future.isDone(), "read completed without majority confirmation");

        for (String peerId : leader.getPeerIds()) {
            cluster.heal(peerId);
        }
        GetResponse getResponse = future.get(5, TimeUnit.SECONDS);
        assertTrue(getResponse.found(), "read did not see the already-committed write after healing");
        assertEquals(value, new String(getResponse.value(), StandardCharsets.UTF_8), "read returned a different value than what was set");
    }
}
