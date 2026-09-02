package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.node.Role;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicationTest {
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
        nodes.forEach(RaftNode::stop);
    }

    @Test
    void basicReplication() throws InterruptedException {
        RaftNodeImpl leader = (RaftNodeImpl) RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15)).orElseThrow(() -> new AssertionError("no leader elected within timeout"));
        byte [] command1 = "command1".getBytes();
        RaftNodeImpl.IndexTerm indexTerm = leader.submit0(command1);
        long index = indexTerm.index();
        LogEntry leadersEntry = leader.getEntryAt0(index).get();

        boolean logEntryReplicated = RaftTestUtils.awaitCondition(Duration.ofSeconds(5), () -> {
            for (RaftNode node : nodes) {
                Optional<LogEntry> oentry = ((RaftNodeImpl)node).getEntryAt0(index);
                if (oentry.isEmpty()) {
                    return false;
                }
                LogEntry entry = oentry.get();
                if (entry.term() != leadersEntry.term() || !Arrays.equals(leadersEntry.command(), entry.command())) {
                    return false;
                }
            }
            return true;
        });

        assertTrue(logEntryReplicated, "Replication of logEntry has failed.");

        boolean commitIndexReplicated = RaftTestUtils.awaitCondition(Duration.ofSeconds(5), () -> {
            for (RaftNode node : nodes) {
                if (node.getCommitIndex() != index) {
                    return false;
                }
            }
            return true;
        });

        assertTrue(commitIndexReplicated, "Replication of commit index has failed.");
    }

    @Test
    void multipleEntriesReplicateInOrder() throws InterruptedException {
        RaftNodeImpl leader = (RaftNodeImpl) RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));

        List<LogEntry> leadersEntries = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            byte[] command = ("command" + i).getBytes();
            RaftNodeImpl.IndexTerm indexTerm = leader.submit0(command);
            leadersEntries.add(leader.getEntryAt0(indexTerm.index()).get());
        }
        long lastIndex = leadersEntries.get(leadersEntries.size() - 1).index();

        boolean allReplicatedInOrder = RaftTestUtils.awaitCondition(Duration.ofSeconds(5), () -> {
            for (RaftNode node : nodes) {
                for (LogEntry expected : leadersEntries) {
                    Optional<LogEntry> oentry = ((RaftNodeImpl)node).getEntryAt0(expected.index());
                    if (oentry.isEmpty()) {
                        return false;
                    }
                    LogEntry actual = oentry.get();
                    if (actual.term() != expected.term() || !Arrays.equals(actual.command(), expected.command())) {
                        return false;
                    }
                }
            }
            return true;
        });
        assertTrue(allReplicatedInOrder, "Not all entries replicated in order to every node.");

        boolean commitIndexAdvanced = RaftTestUtils.awaitCondition(Duration.ofSeconds(5), () -> {
            for (RaftNode node : nodes) {
                if (node.getCommitIndex() != lastIndex) {
                    return false;
                }
            }
            return true;
        });
        assertTrue(commitIndexAdvanced, "commitIndex did not advance to the last submitted entry on every node.");
    }

    @Test
    void isolatedFollowerCatchesUpAfterHealing() throws InterruptedException {
        RaftNodeImpl leader = (RaftNodeImpl) RaftTestUtils.awaitLeader(nodes, Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("no leader elected within timeout"));

        RaftNodeImpl isolatedFollower = (RaftNodeImpl) nodes.stream()
                .filter(n -> n.getRole() != Role.LEADER)
                .findAny()
                .orElseThrow(() -> new AssertionError("no follower to isolate"));
        cluster.isolate(isolatedFollower.getId());

        List<RaftNode> connectedNodes = nodes.stream()
                .filter(n -> !n.getId().equals(isolatedFollower.getId()))
                .toList();

        List<LogEntry> leadersEntries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            byte[] command = ("command" + i).getBytes();
            RaftNodeImpl.IndexTerm indexTerm = leader.submit0(command);
            leadersEntries.add(leader.getEntryAt0(indexTerm.index()).get());
        }
        long lastIndex = leadersEntries.get(leadersEntries.size() - 1).index();

        boolean committedWithoutIsolatedFollower = RaftTestUtils.awaitCondition(Duration.ofSeconds(5), () -> {
            for (RaftNode node : connectedNodes) {
                if (node.getCommitIndex() != lastIndex) {
                    return false;
                }
            }
            return true;
        });
        assertTrue(committedWithoutIsolatedFollower,
                "Entries did not commit on the connected majority while a follower was isolated.");

        cluster.heal(isolatedFollower.getId());

        // generous timeout: while isolated, this node's own election timer keeps firing with
        // nobody to grant it a vote, so its term can climb well past the real leader's. Healing
        // can therefore force an extra election round (the real leader steps down on seeing the
        // higher term in the rejection response) before replication resumes, on top of the
        // normal latency for this node to catch up on the entries it missed.
        boolean isolatedFollowerCaughtUp = RaftTestUtils.awaitCondition(Duration.ofSeconds(15), () -> {
            for (LogEntry expected : leadersEntries) {
                Optional<LogEntry> oentry = isolatedFollower.getEntryAt0(expected.index());
                if (oentry.isEmpty()) {
                    return false;
                }
                LogEntry actual = oentry.get();
                if (actual.term() != expected.term() || !Arrays.equals(actual.command(), expected.command())) {
                    return false;
                }
            }
            return isolatedFollower.getCommitIndex() == lastIndex;
        });
        assertTrue(isolatedFollowerCaughtUp, "Isolated follower did not catch up to the full log after healing.");
    }
}
