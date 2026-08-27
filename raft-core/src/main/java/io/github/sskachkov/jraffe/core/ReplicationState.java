package io.github.sskachkov.jraffe.core;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Per-leadership-term state: each peer's replication progress plus the majority-aggregate
 * queries (majorityMatchIndex, majorityConfirmedDispatchAt) built on it. Recreated fresh on
 * every election — purely a data holder, no active behavior of its own.
 */
public class ReplicationState {

    public record PeerProgress(long nextIndex, long matchIndex, long lastAckedReqId, long lastConfirmedDispatchAt) {}

    private final AtomicLong reqIdCounter;
    private final Map<String, PeerProgress> peerState;

    public ReplicationState(AtomicLong reqIdCounter, List<String> peerIds, long leaderLastIndex) {
        this.reqIdCounter = reqIdCounter;
        this.peerState = new HashMap<>();

        for (String peerId : peerIds) {
            peerState.put(peerId, new PeerProgress(leaderLastIndex + 1, 0, this.nextRequestId(), 0));
        }
    }

    public synchronized PeerProgress getPeerProgress(String peerId) {
        return this.peerState.get(peerId);
    }

    public synchronized void recordSuccess(String peerId, long reqId, long matchedThroughIndex, long dispatchedAt) {
        PeerProgress oldProgress = this.peerState.get(peerId);
        if (reqId <= oldProgress.lastAckedReqId()) {
            return;
        }
        this.peerState.put(peerId, new PeerProgress(matchedThroughIndex + 1, matchedThroughIndex, reqId, dispatchedAt));
    }

    public synchronized void recordFailure(String peerId, long reqId, long dispatchedAt) {
        PeerProgress oldProgress = this.peerState.get(peerId);
        if (reqId <= oldProgress.lastAckedReqId()) {
            return;
        }
        this.peerState.put(peerId, new PeerProgress(Math.max(1, oldProgress.nextIndex() - 1), oldProgress.matchIndex(), reqId, dispatchedAt));
    }

    public synchronized long majorityMatchIndex(long leaderIndex) {
        List<Long> indexes = new ArrayList<>();
        indexes.add(leaderIndex);
        for (PeerProgress peerProg: this.peerState.values()) {
            indexes.add(peerProg.matchIndex());
        }
        Collections.sort(indexes);
        int majIndex = ((indexes.size() + 1) / 2);
        return indexes.get(majIndex - 1);
    }

    public synchronized long majorityConfirmedDispatchAt(long leaderTime) {
        List<Long> times = new ArrayList<>();
        times.add(leaderTime);
        for (PeerProgress peerProg : this.peerState.values()) {
            times.add(peerProg.lastConfirmedDispatchAt());
        }
        Collections.sort(times);
        int majIndex = ((times.size() + 1) / 2);
        return times.get(majIndex - 1);
    }


    public long nextRequestId() {
        return reqIdCounter.incrementAndGet();
    }

}
