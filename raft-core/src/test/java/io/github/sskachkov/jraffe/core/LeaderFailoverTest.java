package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
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
    void newLeaderIsElectedOnANewTermAfterTheLeaderCrashes() throws InterruptedException {
        RaftNode firstLeader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        long firstTerm = firstLeader.getCurrentTerm();

        cluster.crash(firstLeader.getNodeId());

        List<RaftNode> survivors = nodes.stream()
                .filter(n -> !n.getNodeId().equals(firstLeader.getNodeId()))
                .toList();

        RaftNode secondLeader = RaftTestUtils.awaitLeader(survivors, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no new leader elected after the old leader crashed"));

        assertNotEquals(firstLeader.getNodeId(), secondLeader.getNodeId());
        assertTrue(secondLeader.getCurrentTerm() > firstTerm,
                "expected new leader's term (" + secondLeader.getCurrentTerm() + ") to exceed old leader's term (" + firstTerm + ")");
    }

    @Test
    void isolatedLeaderStepsDownAfterPartitionHeals() throws InterruptedException {
        RaftNode oldLeader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        long oldTerm = oldLeader.getCurrentTerm();

        // partition it, but leave it fully running -- unlike crash(), this is reversible
        cluster.isolate(oldLeader.getNodeId());

        List<RaftNode> survivors = nodes.stream()
                .filter(n -> !n.getNodeId().equals(oldLeader.getNodeId()))
                .toList();

        RaftNode newLeader = RaftTestUtils.awaitLeader(survivors, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no new leader elected while old leader was isolated"));
        assertTrue(newLeader.getCurrentTerm() > oldTerm,
                "expected new leader's term (" + newLeader.getCurrentTerm() + ") to exceed old leader's term (" + oldTerm + ")");

        cluster.heal(oldLeader.getNodeId());

        boolean steppedDown = RaftTestUtils.awaitCondition(Duration.ofSeconds(15), () ->
                oldLeader.getRole() == Role.FOLLOWER && oldLeader.getCurrentTerm() == newLeader.getCurrentTerm());
        assertTrue(steppedDown, "expected the isolated old leader to step down to FOLLOWER on the new term once healed, "
                + "but role=" + oldLeader.getRole() + " term=" + oldLeader.getCurrentTerm()
                + " (new leader term=" + newLeader.getCurrentTerm() + ")");
    }

    @Test
    void staleCandidateWithInflatedTermIsRejected() throws InterruptedException {
        RaftNode leader = RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        RaftNode.TermIndex termIndex = leader.submit0("command1".getBytes());
        RaftTestUtils.awaitCondition(Duration.ofSeconds(5), () -> leader.getCommitIndex() == termIndex.index());

        // simulates a node that was partitioned away and kept losing elections to itself,
        // inflating its term far past the real cluster's, while never receiving the entry
        // above -- its log is stale even though its term is not
        long inflatedTerm = leader.getCurrentTerm() + 50;
        var staleHighTermRequest = new RaftRpcClient.RequestVoteRequest(inflatedTerm, "impostor", 0, 0);

        var response = leader.handleRequestVote(staleHighTermRequest);

        assertEquals(inflatedTerm, response.term(), "responder should adopt the higher term even while refusing the vote");
        assertFalse(response.voteGranted(), "candidate's stale log should not win a vote despite the higher term");
        assertEquals(Role.FOLLOWER, leader.getRole(), "seeing a higher term should still step the node down from leader");
        assertEquals(inflatedTerm, leader.getCurrentTerm(), "node's own term should now match the term it just saw");
    }
}
