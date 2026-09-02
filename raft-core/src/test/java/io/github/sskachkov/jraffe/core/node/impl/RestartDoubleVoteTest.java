package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.rpc.EnvelopeFactory;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Isolates Raft's persistence requirement in the smallest possible reproduction: a server must
// never vote for two different candidates in the same term. Today nothing enforces that across a
// restart, since currentTerm/votedFor live only in memory - this is the split-brain precondition
// (two leaders elected in the same term) that persisting them before replying is meant to prevent.
class RestartDoubleVoteTest {
    private static final List<String> NODE_IDS = List.of("n1", "n2", "n3");
    private InMemoryCluster cluster;
    private List<RaftNode> nodes;
    private EnvelopeFactory envelopeFactory;

    @BeforeEach
    void setUp() {
        cluster = new InMemoryCluster();
        nodes = cluster.start(NODE_IDS);
        envelopeFactory = new EnvelopeFactory();
    }

    @AfterEach
    void tearDown() {
        cluster.shutdownAll();
    }

    void restartedNodeMustNotGrantASecondVoteInATermItAlreadyVotedIn() {
        RaftNode n1 = nodes.get(0);
        // n1 grants candidate "b" its vote for term 5.
        var reqFromB = envelopeFactory.create("b", "n1", new RequestVoteRequest(5,  0, 0));
        var respToB = n1.handleRequestVote(reqFromB).payload();
        assertTrue(respToB.voteGranted(), "first vote request in term 5 should be granted");

        // n1 crashes and restarts. With no persistence, the replacement instance has no memory
        // of ever having voted -- currentTerm/votedFor both reset to their initial values.
        RaftNode restarted = cluster.restart("n1");

        // A different candidate asks for n1's vote, in the SAME term 5.
        var reqFromC = envelopeFactory.create("c", "n1", new RequestVoteRequest(5, 0, 0));
        var respToC = restarted.handleRequestVote(reqFromC).payload();

        assertFalse(respToC.voteGranted(),
                "restarted node granted a second, conflicting vote in a term it already voted in -- "
                        + "both \"b\" and \"c\" now believe they hold n1's vote for term 5, "
                        + "which can produce two leaders in the same term");
    }
}
