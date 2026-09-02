package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.rpc.*;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Manages outgoing AppendEntries dispatch for a leader: per-peer heartbeat scheduling,
 * keeping at most one AppendEntries request in flight per peer at a time.
 * Reports each response back to RaftNode.onAppendEntriesResponse for interpretation.
 */
public class RaftNodeReplicator {
    static class PeerDispatchState {
        private boolean inFlight;
        private boolean nextRequested;

        synchronized boolean tryStart() {
            if (!inFlight && nextRequested) {
                inFlight = true;
                nextRequested = false;
                return true;
            }
            return false;
        }
        synchronized void complete() {
            this.inFlight = false;
        }
        synchronized void queueNextRequest() {
            this.nextRequested = true;
        }
        synchronized boolean isNextRequested() {
            return this.nextRequested;
        }
    }

    private final Logger log;
    private final List<String> peerIds;
    private final RaftNodeImpl raftNode;
    private final RaftLog raftLog;
    private final Map<String, PeerDispatchState> dispatchStates;
    private final ReplicationState replicationState;

    private final ExecutorService taskExecutor;
    private final ScheduledExecutorService scheduledExecutorService;

    private volatile ScheduledFuture<?> heartbeatSchedule;

    public RaftNodeReplicator(
            List<String> peerIds,
            RaftNodeImpl raftNode,
            RaftLog raftLog, ExecutorService taskExecutor,
            ScheduledExecutorService scheduledExecutorService,
            long lastAckedReqId) {
        this.log = new ContextAwareLogger(RaftNodeReplicator.class, raftNode.getId());
        this.peerIds = peerIds;
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.taskExecutor = taskExecutor;
        this.scheduledExecutorService = scheduledExecutorService;

        this.replicationState = new ReplicationState(peerIds, this.raftLog.lastIndex(), lastAckedReqId);

        this.dispatchStates = new HashMap<>();
        for (String peerId : this.peerIds) {
            this.dispatchStates.put(peerId, new PeerDispatchState());
        }
    }

    public void enqueueAppendEntriesToAllPeers() {
        for (String peerId : this.peerIds) {
            PeerDispatchState dispatchState = this.dispatchStates.get(peerId);
            dispatchState.queueNextRequest();
            sendAppendEntries(peerId);
        }
    }

    private void sendAppendEntries(String peerId) {
        synchronized (this.raftNode) {
            PeerDispatchState dispatchState = this.dispatchStates.get(peerId);
            if (!dispatchState.tryStart()) { // another request in-flight
                return;
            }
            long currentTerm  = this.raftNode.getCurrentTerm();
            ReplicationState.PeerProgress peerProgress = this.replicationState.getPeerProgress(peerId);
            log.debug("Sending appendEntries to peer: {}, term: {}", peerId, currentTerm);
            long prevLogIndex = peerProgress.nextIndex() - 1;
            long prevLogTerm = this.raftLog.termAt(prevLogIndex);
            List<LogEntry> entries = this.raftLog.entriesFrom(prevLogIndex + 1);

            AppendEntriesRequest req = new AppendEntriesRequest(
                    currentTerm, prevLogIndex, prevLogTerm, entries, this.raftNode.getCommitIndex());
            raftNode.sendAppendEntries(peerId, req);
        }
    }

    public void appendReqSuccess(RpcEnvelope<AppendEntriesRequest> reqEnv, RpcEnvelope<AppendEntriesResponse> respEnv, long dispatchedAt) {
        log.debug("appendReqSuccess {}", respEnv);
        AppendEntriesRequest req = reqEnv.payload();
        AppendEntriesResponse response = respEnv.payload();
        String peerId = reqEnv.recipient();
        long correlationId = respEnv.correlationId();
        try {
            synchronized (this.raftNode) {
                if (response.success()) {
                    long matchedThroughIndex = req.prevLogIndex() + req.entries().size();
                    this.replicationState.recordSuccess(peerId, correlationId, matchedThroughIndex, dispatchedAt);
                } else {
                    this.replicationState.recordFailure(peerId, correlationId, dispatchedAt);
                }
                long leaderTime = System.nanoTime();
                long majorityConfirmedAt = this.replicationState.majorityConfirmedDispatchAt(leaderTime);
                this.raftNode.majorityConfirmedAt(majorityConfirmedAt);
            }
        } finally {
            PeerDispatchState dispatchState = dispatchStates.get(peerId);
            dispatchState.complete();
            if (dispatchState.isNextRequested()) {
                sendAppendEntries(peerId);
            }
        }
    }

    public void appendReqFailure(RpcEnvelope<AppendEntriesRequest> envelope) {
        String peerId = envelope.recipient();
        PeerDispatchState dispatchState = dispatchStates.get(peerId);
        dispatchState.complete();
        if (dispatchState.isNextRequested()) {
            sendAppendEntries(peerId);
        }
    }

    public void start() {
        this.scheduleHeartbeat();
    }

    public void stop() {
        this.heartbeatSchedule.cancel(false);
    }

    private void scheduleHeartbeat() {
        synchronized (this.raftNode) {
            if (!this.raftNode.isRunning()) {
                return;
            }
            if (this.heartbeatSchedule != null) {
                this.heartbeatSchedule.cancel(false);
            }
            this.heartbeatSchedule = scheduledExecutorService.scheduleAtFixedRate(() -> taskExecutor.execute(this::enqueueAppendEntriesToAllPeers), 0, 50, TimeUnit.MILLISECONDS);
        }
    }

    public ReplicationState getReplicationState() {
        return replicationState;
    }

    public long majorityMatchIndex(long leaderIndex) {
        return replicationState.majorityMatchIndex(leaderIndex);
    }

}
