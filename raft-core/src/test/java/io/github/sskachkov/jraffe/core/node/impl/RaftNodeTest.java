package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.node.Role;
import io.github.sskachkov.jraffe.core.rpc.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftNodeTest {
    private EnvelopeFactory envelopeFactory;
    @BeforeEach
    void setUp() {
        envelopeFactory = new EnvelopeFactory();
    }


    @Test
    void becomeFollowerOnSameTermAppendEntriesPreservesVotedFor() throws InterruptedException {
        InMemoryCluster cluster = new InMemoryCluster();
        RaftNodeImpl node = new RaftNodeImpl("n1", List.of("n2"), cluster.clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());
        cluster.add(node);
        cluster.isolate("n2"); // node's only peer is unreachable -- it can never win an election

        try {
            node.start();
            boolean becameCandidate = RaftTestUtils.awaitCondition(Duration.ofSeconds(15),
                    () -> node.getRole() == Role.CANDIDATE);
            assertTrue(becameCandidate, "node should keep retrying and staying a candidate with its only peer unreachable");

            long term = node.getCurrentTerm();
            RpcEnvelope<AppendEntriesRequest> reqEnv = envelopeFactory.create("n2", "n1",
                    new AppendEntriesRequest(term, 0, 0, List.of(), 0));
            var response = node.handleAppendEntries(reqEnv).payload();

            assertTrue(response.success());
            assertEquals(Role.FOLLOWER, node.getRole());
            assertEquals("n1", node.getVotedFor0(),
                    "stepping down on an equal-term message must not clear the vote it already cast this term");
        } finally {
            node.stop();
        }
    }

    @Test
    void secondVoteRequestInSameTermFromDifferentCandidateIsRejected() {
        RaftNodeImpl node = new RaftNodeImpl("n1", List.of("n2", "n3"), new InMemoryCluster().clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());

        RpcEnvelope<RequestVoteRequest> reqFromA = envelopeFactory.create("n2", "n1",
                new RequestVoteRequest(1, 0, 0));
        var responseToA = node.handleRequestVote(reqFromA).payload();
        assertTrue(responseToA.voteGranted());

        RpcEnvelope<RequestVoteRequest> reqFromB = envelopeFactory.create("n3", "n1",
                new RequestVoteRequest(1, 0, 0));
        var responseToB = node.handleRequestVote(reqFromB).payload();
        assertFalse(responseToB.voteGranted(), "should not grant a second vote in the same term to a different candidate");
    }

    @Test
    void handleAppendEntriesRejectsStaleTerm() {
        RaftNodeImpl node = new RaftNodeImpl("n1", List.of("n2", "n3"), new InMemoryCluster().clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());
        // bump the node's term via a vote request so there's a nonzero term for the request below to be stale relative to
        RpcEnvelope<RequestVoteRequest> bumpTermReq = envelopeFactory.create("n2", "n1",
                new RequestVoteRequest(5, 0, 0));
        node.handleRequestVote(bumpTermReq);

        var staleRequest = envelopeFactory.create("n3", "n1",
                new AppendEntriesRequest(2, 0, 0, List.of(), 0));
        var response = node.handleAppendEntries(staleRequest).payload();

        assertFalse(response.success());
        assertEquals(5, response.term(), "response should report the node's real, higher current term, not the stale request's");
    }

    @Test
    void handleAppendEntriesRejectsMismatchedPrevLogTerm() {
        RaftNodeImpl node = new RaftNodeImpl("n1", List.of("n2"), new InMemoryCluster().clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());

        var mismatchedRequest = envelopeFactory.create("n2", "n1",
                new AppendEntriesRequest(0, 0, 99, List.of(), 0));
        var response = node.handleAppendEntries(mismatchedRequest).payload();

        assertFalse(response.success());
    }

}
