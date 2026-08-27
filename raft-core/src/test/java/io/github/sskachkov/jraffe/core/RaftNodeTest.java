package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftNodeTest {

    @Test
    void becomeFollowerOnSameTermAppendEntriesPreservesVotedFor() throws InterruptedException {
        InMemoryCluster cluster = new InMemoryCluster();
        RaftNode node = new RaftNode("n1", List.of("n2"), cluster.clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());
        cluster.add(node);
        cluster.isolate("n2"); // node's only peer is unreachable -- it can never win an election

        try {
            node.start();
            boolean becameCandidate = RaftTestUtils.awaitCondition(Duration.ofSeconds(15),
                    () -> node.getRole() == Role.CANDIDATE);
            assertTrue(becameCandidate, "node should keep retrying and staying a candidate with its only peer unreachable");

            long term = node.getCurrentTerm();
            var sameTermAppendEntries = new RaftRpcClient.AppendEntriesRequest(1, term, "n2", 0, 0, List.of(), 0);
            var response = node.handleAppendEntries(sameTermAppendEntries);

            assertTrue(response.success());
            assertEquals(Role.FOLLOWER, node.getRole());
            assertEquals("n1", node.getVotedFor0(),
                    "stepping down on an equal-term message must not clear the vote it already cast this term");
        } finally {
            node.shutdown();
        }
    }

    @Test
    void secondVoteRequestInSameTermFromDifferentCandidateIsRejected() {
        RaftNode node = new RaftNode("n1", List.of("n2", "n3"), new InMemoryCluster().clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());

        var requestFromA = new RaftRpcClient.RequestVoteRequest(1, "n2", 0, 0);
        var responseToA = node.handleRequestVote(requestFromA);
        assertTrue(responseToA.voteGranted());

        var requestFromB = new RaftRpcClient.RequestVoteRequest(1, "n3", 0, 0);
        var responseToB = node.handleRequestVote(requestFromB);
        assertFalse(responseToB.voteGranted(), "should not grant a second vote in the same term to a different candidate");
    }

    @Test
    void handleAppendEntriesRejectsStaleTerm() {
        RaftNode node = new RaftNode("n1", List.of("n2"), new InMemoryCluster().clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());
        // bump the node's term via a vote request so there's a nonzero term for the request below to be stale relative to
        node.handleRequestVote(new RaftRpcClient.RequestVoteRequest(5, "n2", 0, 0));

        var staleRequest = new RaftRpcClient.AppendEntriesRequest(1, 2, "impostor", 0, 0, List.of(), 0);
        var response = node.handleAppendEntries(staleRequest);

        assertFalse(response.success());
        assertEquals(5, response.term(), "response should report the node's real, higher current term, not the stale request's");
    }

    @Test
    void handleAppendEntriesRejectsMismatchedPrevLogTerm() {
        RaftNode node = new RaftNode("n1", List.of("n2"), new InMemoryCluster().clientFor("n1"), new TestStateMachineAdapter(), new SimpleMeterRegistry());

        var mismatchedRequest = new RaftRpcClient.AppendEntriesRequest(1, 0, "n2", 0, 99, List.of(), 0);
        var response = node.handleAppendEntries(mismatchedRequest);

        assertFalse(response.success());
    }

}
