package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftLogTest {

    // -- isCandidateUpToDate (§5.4.1): term dominates, index only breaks a tie --

    @Test
    void higherTermWinsEvenWithFewerEntries() {
        RaftLog log = new RaftLog("n1");
        log.appendNew(3, "x".getBytes(), false); // voter: index 1, term 3
        assertTrue(log.candidateUpToDate(0, 4)); // candidate: behind on index, ahead on term
    }

    @Test
    void lowerTermLosesEvenWithMoreEntries() {
        RaftLog log = new RaftLog("n1");
        log.appendNew(5, "x".getBytes(), false); // voter: index 1, term 5
        assertFalse(log.candidateUpToDate(100, 2)); // candidate: way ahead on index, behind on term
    }

    @Test
    void sameTermCandidateAheadOnIndexWins() {
        RaftLog log = new RaftLog("n1");
        log.appendNew(3, "x".getBytes(), false); // voter: index 1, term 3
        assertTrue(log.candidateUpToDate(5, 3));
    }

    @Test
    void sameTermCandidateBehindOnIndexLoses() {
        RaftLog log = new RaftLog("n1");
        log.appendNew(3, "x".getBytes(), false);
        log.appendNew(3, "y".getBytes(), false); // voter: index 2, term 3
        assertFalse(log.candidateUpToDate(1, 3));
    }

    @Test
    void exactTieCounts() {
        RaftLog log = new RaftLog("n1");
        log.appendNew(3, "x".getBytes(), false); // voter: index 1, term 3
        assertTrue(log.candidateUpToDate(1, 3)); // identical logs count as "at least as up to date"
    }

    @Test
    void emptyLogsAreEquallyUpToDate() {
        RaftLog log = new RaftLog("n1");
        assertTrue(log.candidateUpToDate(0, 0));
    }

    // -- tryAppend (§5.3): consistency check, idempotent retries, conflict truncation --

    @Test
    void rejectsWhenPrevLogIndexBeyondEnd() {
        RaftLog log = new RaftLog("n1"); // lastIndex() == 0
        boolean accepted = log.tryAppend(5, 1, List.of(new LogEntry(6, 1, "x".getBytes(), false)));
        assertFalse(accepted);
        assertEquals(0, log.lastIndex()); // untouched
    }

    @Test
    void rejectsWhenPrevLogTermMismatches() {
        RaftLog log = new RaftLog("n1");
        log.appendNew(3, "a".getBytes(), false); // index 1, term 3
        boolean accepted = log.tryAppend(1, 99, List.of(new LogEntry(2, 3, "b".getBytes(), false)));
        assertFalse(accepted);
        assertEquals(1, log.lastIndex()); // untouched
    }

    @Test
    void rejectsWhenPrevLogIndexIsNegative() {
        RaftLog log = new RaftLog("n1");
        boolean accepted = log.tryAppend(-1, 1, List.of(new LogEntry(0, 0, "x".getBytes(), false)));
        assertFalse(accepted);
    }

    @Test
    void appendsFreshEntriesPastTheEnd() {
        RaftLog log = new RaftLog("n1");
        boolean accepted = log.tryAppend(0, 0, List.of(
                new LogEntry(1, 1, "a".getBytes(), false),
                new LogEntry(2, 1, "b".getBytes(), false)));

        assertTrue(accepted);
        assertEquals(2, log.lastIndex());
        assertArrayEquals("a".getBytes(), log.entryAt(1).command());
        assertArrayEquals("b".getBytes(), log.entryAt(2).command());
    }

    @Test
    void retryWithIdenticalEntriesIsIdempotent() {
        RaftLog log = new RaftLog("n1");
        boolean accepted = log.tryAppend(0, 0, List.of(new LogEntry(1, 1, "a".getBytes(), false), new LogEntry(2, 1, "b".getBytes(), false)));
        assertTrue(accepted);

        boolean acceptedAgain = log.tryAppend(0, 0, List.of(
                new LogEntry(1, 1, "a".getBytes(), false), new LogEntry(2, 1, "b".getBytes(), false)));

        assertTrue(acceptedAgain);
        assertEquals(2, log.lastIndex()); // no duplication
        assertArrayEquals("a".getBytes(), log.entryAt(1).command());
        assertArrayEquals("b".getBytes(), log.entryAt(2).command());
    }

    @Test
    void conflictTruncatesExistingTailAndAppendsNew() {
        RaftLog log = new RaftLog("n1");
        log.tryAppend(0, 0, List.of(
                new LogEntry(1, 1, "old1".getBytes(), false),
                new LogEntry(2, 1, "old2".getBytes(), false),
                new LogEntry(3, 1, "old3".getBytes(), false)));

        // a new leader (term 2) only agrees with the log up to index 0, and sends a
        // conflicting entry at index 1 -- entries 2 and 3 should be discarded, not just
        // entry 1 overwritten in place
        boolean accepted = log.tryAppend(0, 0, List.of(new LogEntry(1, 2, "new1".getBytes(), false)));

        assertTrue(accepted);
        assertEquals(1, log.lastIndex());
        assertEquals(2, log.termAt(1));
        assertArrayEquals("new1".getBytes(), log.entryAt(1).command());
    }

    @Test
    void unmentionedTailBeyondBatchIsLeftUntouched() {
        RaftLog log = new RaftLog("n1");
        log.tryAppend(0, 0, List.of(
                new LogEntry(1, 1, "a".getBytes(), false),
                new LogEntry(2, 1, "b".getBytes(), false),
                new LogEntry(3, 1, "c".getBytes(), false)));

        // a batch that only re-confirms entry 1 (matching term) shouldn't touch entries 2/3,
        // even though it doesn't mention them -- truncation only happens on an actual conflict
        boolean accepted = log.tryAppend(0, 0, List.of(new LogEntry(1, 1, "a".getBytes(), false)));

        assertTrue(accepted);
        assertEquals(3, log.lastIndex()); // entries 2 and 3 still present
        assertArrayEquals("b".getBytes(), log.entryAt(2).command());
        assertArrayEquals("c".getBytes(), log.entryAt(3).command());
    }

    @Test
    void emptyEntriesOnMatchingPrevLogIndexIsNoOp() {
        RaftLog log = new RaftLog("n1");
        log.tryAppend(0, 0, List.of(
                new LogEntry(1, 1, "a".getBytes(), false),
                new LogEntry(2, 1, "b".getBytes(), false)));

        boolean accepted = log.tryAppend(2, 1, List.of()); // heartbeat, prevLogIndex matches the log's end

        assertTrue(accepted);
        assertEquals(2, log.lastIndex()); // unchanged
    }
}
