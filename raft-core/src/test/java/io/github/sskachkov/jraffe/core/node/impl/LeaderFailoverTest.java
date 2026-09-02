package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.node.Role;
import io.github.sskachkov.jraffe.core.rpc.EnvelopeFactory;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteRequest;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteResponse;
import io.github.sskachkov.jraffe.core.rpc.RpcEnvelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderFailoverTest {
    private static final List<String> NODE_IDS = List.of("n1", "n2", "n3", "n4", "n5");
    private InMemoryCluster cluster;
    private List<RaftNode> nodes;
    private EnvelopeFactory envelopeFactory;

    @BeforeEach
    void setUp() {
        cluster = new InMemoryCluster();
        nodes = cluster.start(NODE_IDS);
        this.envelopeFactory = new EnvelopeFactory();
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::stop);
    }

    @Test
    void newLeaderIsElectedOnANewTermAfterTheLeaderCrashes() throws InterruptedException {
        RaftNode firstLeader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        long firstTerm = firstLeader.getCurrentTerm();

        cluster.crash(firstLeader.getId());

        List<RaftNode> survivors = nodes.stream()
                .filter(n -> !n.getId().equals(firstLeader.getId()))
                .toList();

        RaftNode secondLeader = RaftTestUtils.awaitLeader(survivors, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no new leader elected after the old leader crashed"));

        assertNotEquals(firstLeader.getId(), secondLeader.getId());
        assertTrue(secondLeader.getCurrentTerm() > firstTerm,
                "expected new leader's term (" + secondLeader.getCurrentTerm() + ") to exceed old leader's term (" + firstTerm + ")");
    }

    @Test
    void isolatedLeaderStepsDownAfterPartitionHeals() throws InterruptedException {
        RaftNode oldLeader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        long oldTerm = oldLeader.getCurrentTerm();

        // partition it, but leave it fully running -- unlike crash(), this is reversible
        cluster.isolate(oldLeader.getId());

        List<RaftNode> survivors = nodes.stream()
                .filter(n -> !n.getId().equals(oldLeader.getId()))
                .toList();

        RaftNode newLeader = RaftTestUtils.awaitLeader(survivors, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no new leader elected while old leader was isolated"));
        assertTrue(newLeader.getCurrentTerm() > oldTerm,
                "expected new leader's term (" + newLeader.getCurrentTerm() + ") to exceed old leader's term (" + oldTerm + ")");

        cluster.heal(oldLeader.getId());

        boolean steppedDown = RaftTestUtils.awaitCondition(Duration.ofSeconds(15), () ->
                oldLeader.getRole() == Role.FOLLOWER && oldLeader.getCurrentTerm() == newLeader.getCurrentTerm());
        assertTrue(steppedDown, "expected the isolated old leader to step down to FOLLOWER on the new term once healed, "
                + "but role=" + oldLeader.getRole() + " term=" + oldLeader.getCurrentTerm()
                + " (new leader term=" + newLeader.getCurrentTerm() + ")");
    }

    @Test
    void staleCandidateWithInflatedTermIsRejected() throws InterruptedException {
        RaftNodeImpl leader = (RaftNodeImpl) RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        RaftNodeImpl.IndexTerm indexTerm = leader.submit0("command1".getBytes());
        RaftTestUtils.awaitCondition(Duration.ofSeconds(5), () -> leader.getCommitIndex() == indexTerm.index());

        // simulates a node that was partitioned away and kept losing elections to itself,
        // inflating its term far past the real cluster's, while never receiving the entry
        // above -- its log is stale even though its term is not
        long inflatedTerm = leader.getCurrentTerm() + 50;
        var staleHighTermRequest = new RequestVoteRequest(inflatedTerm, 0, 0);
        RpcEnvelope<RequestVoteRequest> env = envelopeFactory.create("impostor", leader.getId(), staleHighTermRequest);
        var respEnv = leader.handleRequestVote(env);
        RequestVoteResponse response = respEnv.payload();
        assertEquals(inflatedTerm, response.term(), "responder should adopt the higher term even while refusing the vote");
        assertFalse(response.voteGranted(), "candidate's stale log should not win a vote despite the higher term");
        assertEquals(Role.FOLLOWER, leader.getRole(), "seeing a higher term should still step the node down from leader");
        assertEquals(inflatedTerm, leader.getCurrentTerm(), "node's own term should now match the term it just saw");
    }
}
