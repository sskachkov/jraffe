package io.github.sskachkov.jraffe.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationStateTest {

    // -- initial state --

    @Test
    void initialStateIsOptimisticNextIndexAndZeroMatchIndex() {
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1", "p2"), 10);

        ReplicationState.PeerProgress progress = tracker.getPeerProgress("p1");
        assertEquals(11, progress.nextIndex()); // leaderLastIndex + 1
        assertEquals(0, progress.matchIndex()); // nothing confirmed yet
    }

    @Test
    void constructorSeedsStalenessBaselineFromCounterSoOldReqIdsAreIgnored() {
        AtomicLong counter = new AtomicLong();
        long staleReqId = counter.incrementAndGet(); // minted by a PRIOR tracker/leadership term
        counter.incrementAndGet(); // counter keeps climbing across leadership terms, never resets

        ReplicationState tracker = new ReplicationState(counter, List.of("p1"), 0);

        tracker.recordSuccess("p1", staleReqId, 5, 0); // a response for a request that predates this tracker
        assertEquals(0, tracker.getPeerProgress("p1").matchIndex(), "reqId from before construction should be ignored");
    }

    // -- recordSuccess --

    @Test
    void recordSuccessAdvancesNextIndexAndMatchIndex() {
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1"), 0);

        tracker.recordSuccess("p1", tracker.nextRequestId(), 7, 0);

        ReplicationState.PeerProgress progress = tracker.getPeerProgress("p1");
        assertEquals(8, progress.nextIndex());
        assertEquals(7, progress.matchIndex());
    }

    @Test
    void recordSuccessIgnoresOutOfOrderStaleResponse() {
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1"), 0);

        long olderReqId = tracker.nextRequestId();
        long newerReqId = tracker.nextRequestId();

        tracker.recordSuccess("p1", newerReqId, 10, 0); // the newer response is applied first
        tracker.recordSuccess("p1", olderReqId, 3, 0);  // the older one arrives late and must be ignored

        ReplicationState.PeerProgress progress = tracker.getPeerProgress("p1");
        assertEquals(10, progress.matchIndex());
        assertEquals(11, progress.nextIndex());
    }

    @Test
    void recordSuccessIgnoresDuplicateReqId() {
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1"), 0);

        long reqId = tracker.nextRequestId();
        tracker.recordSuccess("p1", reqId, 10, 0);
        tracker.recordSuccess("p1", reqId, 999, 0); // the same reqId replayed must not be re-applied

        assertEquals(10, tracker.getPeerProgress("p1").matchIndex());
    }

    // -- recordFailure --

    @Test
    void recordFailureDecrementsNextIndexButPreservesMatchIndex() {
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1"), 10); // nextIndex starts at 11

        tracker.recordSuccess("p1", tracker.nextRequestId(), 5, 0); // matchIndex=5, nextIndex=6
        tracker.recordFailure("p1", tracker.nextRequestId(), 0);

        ReplicationState.PeerProgress progress = tracker.getPeerProgress("p1");
        assertEquals(5, progress.nextIndex()); // backed off by one from 6
        assertEquals(5, progress.matchIndex()); // untouched -- a failed probe doesn't un-confirm proven data
    }

    @Test
    void recordFailureNeverGoesBelowOne() {
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1"), 0); // nextIndex starts at 1

        for (int i = 0; i < 5; i++) {
            tracker.recordFailure("p1", tracker.nextRequestId(), 0);
        }

        assertEquals(1, tracker.getPeerProgress("p1").nextIndex());
    }

    @Test
    void recordFailureIgnoresOutOfOrderStaleResponse() {
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1"), 10); // nextIndex starts at 11

        long olderReqId = tracker.nextRequestId();
        long newerReqId = tracker.nextRequestId();

        tracker.recordFailure("p1", newerReqId, 0); // nextIndex: 11 -> 10
        tracker.recordFailure("p1", olderReqId, 0); // arrives late, must be ignored

        assertEquals(10, tracker.getPeerProgress("p1").nextIndex());
    }

    // -- majorityMatchIndex --

    @Test
    void majorityMatchIndexForOddClusterSize() {
        // 5-node cluster: leader + 4 peers, majority = 3
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1", "p2", "p3", "p4"), 0);

        tracker.recordSuccess("p1", tracker.nextRequestId(), 8, 0);
        tracker.recordSuccess("p2", tracker.nextRequestId(), 6, 0);
        tracker.recordSuccess("p3", tracker.nextRequestId(), 4, 0);
        tracker.recordSuccess("p4", tracker.nextRequestId(), 2, 0);

        // combined with leader=10: [2,4,6,8,10] ascending -- position 2 (0-indexed) = 6
        assertEquals(6, tracker.majorityMatchIndex(10));
    }

    @Test
    void majorityMatchIndexForEvenClusterSize() {
        // 4-node cluster: leader + 3 peers, majority = 3
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1", "p2", "p3"), 0);

        tracker.recordSuccess("p1", tracker.nextRequestId(), 8, 0);
        tracker.recordSuccess("p2", tracker.nextRequestId(), 6, 0);
        tracker.recordSuccess("p3", tracker.nextRequestId(), 2, 0);

        // combined with leader=10: [2,6,8,10] ascending -- position 1 (0-indexed) = 6
        assertEquals(6, tracker.majorityMatchIndex(10));
    }

    @Test
    void majorityMatchIndexCountsTheLeadersOwnIndex() {
        // 2-node cluster: leader + 1 peer, majority = 2 -- both must agree
        ReplicationState tracker = new ReplicationState(new AtomicLong(), List.of("p1"), 0);

        tracker.recordSuccess("p1", tracker.nextRequestId(), 20, 0); // peer is way ahead of the leader

        // the leader's own (lower) index is the constraining value once it's required for majority
        assertEquals(5, tracker.majorityMatchIndex(5));
    }
}
