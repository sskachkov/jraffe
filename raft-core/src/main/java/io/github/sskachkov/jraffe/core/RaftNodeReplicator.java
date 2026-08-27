package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
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
    private final String nodeId;
    private final List<String> peerIds;
    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RaftRpcClient rpcClient;
    private final Map<String, PeerDispatchState> dispatchStates;
    private final ExecutorService taskExecutor;

    private final ScheduledExecutorService scheduledExecutorService;
    private volatile ScheduledFuture<?> heartbeatSchedule;

    public RaftNodeReplicator(String nodeId,
                              List<String> peerIds,
                              RaftRpcClient rpcClient,
                              RaftNode raftNode,
                              RaftLog raftLog,
                              MeterRegistry registry) {
        this.log = new ContextAwareLogger(RaftNodeReplicator.class, nodeId);
        this.nodeId = nodeId;
        this.peerIds = peerIds;
        this.rpcClient = rpcClient;
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(4);
        this.scheduledExecutorService = ExecutorServiceMetrics.monitor(registry, scheduler, "raft-scheduler");
        this.taskExecutor = ExecutorServiceMetrics.monitor(registry, Executors.newVirtualThreadPerTaskExecutor(), "raft-taskExecutor");
        this.dispatchStates = new HashMap<>();
        for (String peerId : this.peerIds) {
            this.dispatchStates.put(peerId, new PeerDispatchState());
        }
    }

    public void requestSendAppendEntriesToAllPeers() {
        for (String peerId : this.peerIds) {
            PeerDispatchState dispatchState = this.dispatchStates.get(peerId);
            dispatchState.queueNextRequest();
        }
        if (this.raftNode.isRunning()) {
            this.taskExecutor.execute(this::sendAppendEntriesToAllPeers);
        }
    }

    private void sendAppendEntriesToAllPeers() {
        ReplicationState tracker;
        long currentTermLocal;
        synchronized (this.raftNode) {
            if (!this.raftNode.isRunning() || this.raftNode.getRole() != Role.LEADER) {
                return;
            }
            tracker = this.raftNode.getReplicationState();
            currentTermLocal = this.raftNode.getCurrentTerm();
        }
        log.trace("Sending appendEntries to all peers with term: {}", currentTermLocal);
        long dispatchedAt = System.nanoTime();

        for (String peerId : this.peerIds) {
            PeerDispatchState dispatchState = this.dispatchStates.get(peerId);

            if (!dispatchState.tryStart()) {
                continue;
            }
            log.trace("Sending appendEntries to the node: {} with term: {}", peerId, currentTermLocal);
            long reqId = tracker.nextRequestId();
            ReplicationState.PeerProgress peerProgress = tracker.getPeerProgress(peerId);
            long prevLogIndex = peerProgress.nextIndex() - 1;
            long prevLogTerm = this.raftLog.termAt(prevLogIndex);
            List<LogEntry> entries = this.raftLog.entriesFrom(prevLogIndex + 1);

            RaftRpcClient.AppendEntriesRequest req = new RaftRpcClient.AppendEntriesRequest(
                    reqId, currentTermLocal, nodeId, prevLogIndex, prevLogTerm, entries, this.raftNode.getCommitIndex());
            this.taskExecutor.execute(() -> {
                try {
                    RaftRpcClient.AppendEntriesResponse resp = rpcClient.appendEntries(peerId, req);
                    this.raftNode.onAppendEntriesResponse(peerId, resp, prevLogIndex, entries.size(), dispatchedAt);
                } catch (RaftRpcException e) {
                    log.debug("failed to send AppendEntries to {}: {}", peerId, e.getLocalizedMessage());
                } catch (Exception e) {
                    log.error("Exception while sending AppendEntries to {}", peerId, e);
                } finally {
                    dispatchState.complete();
                    if (dispatchState.isNextRequested()) {
                        this.taskExecutor.execute(this::sendAppendEntriesToAllPeers);
                    }
                }
            });
        }
    }

    public void start() {
        this.scheduleHeartbeat();
    }

    public synchronized void shutdown() {
        this.heartbeatSchedule.cancel(false);
        this.scheduledExecutorService.shutdown();
        this.taskExecutor.shutdown();

    }
    private synchronized void scheduleHeartbeat() {
        if (!this.raftNode.isRunning()) {
            return;
        }

        if (this.heartbeatSchedule != null) {
            this.heartbeatSchedule.cancel(false);
        }
        this.heartbeatSchedule = scheduledExecutorService.scheduleAtFixedRate(() -> taskExecutor.execute(this::requestSendAppendEntriesToAllPeers), 0, 50, TimeUnit.MILLISECONDS);
    }
}
