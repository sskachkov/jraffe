package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.StateMachine;
import io.github.sskachkov.jraffe.core.error.RaftInternalError;
import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.core.node.PeerReplicationStatus;
import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.node.Role;
import io.github.sskachkov.jraffe.core.rpc.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;


/**
 * A single Raft node's full state machine: role/term/log bookkeeping, leader election,
 * replication triggers, and resolution of pending client submissions. Holds the RaftLog and
 * drives an internal RaftNodeReplicator for outgoing replication; transport-agnostic &mdash; talks
 * to peers only through the injected RaftRpcClient.
 */
public class RaftNodeImpl implements RaftNode {
    record FutureResponse(long requestId, CompletableFuture<RaftResponse> future) {}
    record ReadonlyFuture(long requestId, byte[] request, CompletableFuture<RaftResponse> future, long submittedAt) {}
    record IndexTerm(long index, long term) {}

    private final static IndexTerm INVALID_INDEX_TERM = new IndexTerm(-1, -1);
    private final static long REQUEST_TIMEOUT = 1;

    private final Logger log;

    private final String nodeId;
    private final List<String> peerIds;
    private final RaftRpcClient rpcClient;
    private final ExecutorService taskExecutor;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final EnvelopeFactory envelopeFactory;
    private final Map<IndexTerm, FutureResponse> futuresMap;
    private final Queue<ReadonlyFuture> readonlyFutures;
    private final ScheduledExecutorService scheduledExecutor;
    private final RaftNodeElectionScheduler electionScheduler;

    private volatile RaftNodeReplicator replicator;
    private volatile boolean running;
    private volatile String votedFor;
    private volatile long currentTerm;
    private volatile Role role;
    private volatile String leaderId;
    private volatile long commitIndex;
    private volatile long lastApplied;

    public RaftNodeImpl(String nodeId, List<String> peerIds, RaftRpcClient rpcClient, StateMachine stateMachine, MeterRegistry registry) {
        this.nodeId = nodeId;
        this.peerIds = Collections.unmodifiableList(peerIds);
        this.rpcClient = rpcClient;
        this.role = Role.FOLLOWER;
        this.currentTerm = 0;
        this.stateMachine = stateMachine;

        this.log = new ContextAwareLogger(RaftNodeImpl.class, this.nodeId);

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(4);
        this.scheduledExecutor = ExecutorServiceMetrics.monitor(registry, scheduler, "raft-scheduler");
        this.futuresMap = new ConcurrentHashMap<>();
        this.readonlyFutures = new ConcurrentLinkedQueue<>();

        this.raftLog = new RaftLog(this.nodeId);
        this.envelopeFactory = new EnvelopeFactory();
        this.electionScheduler = new RaftNodeElectionScheduler(this, scheduledExecutor);
        this.taskExecutor = ExecutorServiceMetrics.monitor(registry, Executors.newVirtualThreadPerTaskExecutor(), "raft-taskExecutor");
    }

    public void beginElection() {
        if (!this.running) {
            return;
        }
        taskExecutor.execute(this::performElection);
    }

    void sendAppendEntries(String peerId, AppendEntriesRequest req) {
        if (!this.running || this.role != Role.LEADER) {
            return;
        }
        RpcEnvelope<AppendEntriesRequest> envelope = this.envelopeFactory.create(this.nodeId, peerId, req);
        taskExecutor.execute(() -> {
            this.performSendAppendEntries(envelope);
        });
    }
    public RaftResponse submitSync(RaftMessage request) {
        return await(submit(request), request.id());
    }
    public RaftResponse submitReadonlySync(RaftMessage request) {
        return await(submitReadonly(request), request.id());
    }
    private RaftResponse await(CompletableFuture<RaftResponse> future, long requestId) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            log.error("Unexpected failure while awaiting submit reqId={}", requestId, e.getCause());
            return RaftResponse.failure(requestId, this.nodeId, new RaftInternalError(String.valueOf(e.getCause())));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RaftResponse.failure(requestId, this.nodeId, new RaftInternalError("interrupted"));
        }
    }

     public synchronized CompletableFuture<RaftResponse> submit(RaftMessage request) {
        IndexTerm indexTerm = submit0(request.data());
        if (indexTerm.equals(INVALID_INDEX_TERM)) {
            log.debug("Rejecting submit reqId={}: not leader (term={}, leaderId={})", request.id(), this.currentTerm, this.leaderId);
            return CompletableFuture.completedFuture(RaftResponse.notLeaderError(request.id(), this.nodeId, this.leaderId));
        }
        log.debug("Accepting submit reqId={} at index={} term={}", request.id(), indexTerm.index, indexTerm.term);
        CompletableFuture<RaftResponse> future = new CompletableFuture<>();
        future.completeOnTimeout(RaftResponse.timeoutError(request.id(), this.nodeId), REQUEST_TIMEOUT, TimeUnit.SECONDS);
        future.whenComplete((RaftResponse rr, Throwable t) -> {
            this.futuresMap.remove(indexTerm);
        });
        this.futuresMap.put(indexTerm, new FutureResponse(request.id(), future));
        return future;
    }


    public synchronized CompletableFuture<RaftResponse> submitReadonly(RaftMessage request) {
        if (this.role != Role.LEADER) {
            log.debug("Rejecting readonly submit reqId={}: not leader (term={}, leaderId={})", request.id(), this.currentTerm, this.leaderId);
            return CompletableFuture.completedFuture(RaftResponse.notLeaderError(request.id(), this.nodeId, this.leaderId));
        }
        log.debug("Queued readonly submit reqId={} term={}", request.id(), this.currentTerm);
        CompletableFuture<RaftResponse> future = new CompletableFuture<>();
        future.completeOnTimeout(RaftResponse.timeoutError(request.id(), this.nodeId), REQUEST_TIMEOUT, TimeUnit.SECONDS);
        long nanoTime = System.nanoTime();
        this.readonlyFutures.add(new ReadonlyFuture(request.id(), request.data(), future, nanoTime));
        return future;
    }

    public synchronized RpcEnvelope<RequestVoteResponse> handleRequestVote(RpcEnvelope<RequestVoteRequest> reqEnv) {
        RequestVoteRequest request = reqEnv.payload();
        if (request.term() > this.currentTerm) {
            becomeFollower(request.term(), null);
        }

        if ((this.votedFor != null && !this.votedFor.equals(reqEnv.sender())) ||
                this.currentTerm > request.term() ||
                !raftLog.candidateUpToDate(request.lastLogIndex(), request.lastLogTerm())) {
            log.debug("Rejecting vote in election term {} to the node {}", this.currentTerm, reqEnv.sender());
            return envelopeFactory.response(reqEnv, new RequestVoteResponse(this.currentTerm, false));
        }

        this.electionScheduler.resetElectionTimer(); // since another vote is in progress, reset current node election timer
        log.debug("granting vote in election term {} to the node {}", this.currentTerm, reqEnv.sender());
        this.votedFor = reqEnv.sender();
        return envelopeFactory.response(reqEnv, new RequestVoteResponse(this.currentTerm, true));
    }

    public synchronized RpcEnvelope<AppendEntriesResponse> handleAppendEntries(RpcEnvelope<AppendEntriesRequest> env) {
        log.debug("Processing append entries request: {}", env);
        AppendEntriesRequest req = env.payload();
        if (req.term() < this.currentTerm) {
            return envelopeFactory.response(env, new AppendEntriesResponse(this.currentTerm, false));
        }
        this.electionScheduler.resetElectionTimer(); // active leader present, reset election timer

        if (req.term() > this.currentTerm || this.role == Role.CANDIDATE) {
            becomeFollower(req.term(), env.sender());
        }
        this.leaderId = env.sender();

        boolean appendSuccess = raftLog.tryAppend(req.prevLogIndex(), req.prevLogTerm(), req.entries());
        if (!appendSuccess) {
            return envelopeFactory.response(env, new AppendEntriesResponse(this.currentTerm, false));
        }

        if (this.commitIndex < req.leaderCommit()) {
            this.commitIndex = Math.min(req.leaderCommit(), raftLog.lastIndex());
            this.applyCommitedEntries();
        }

        return envelopeFactory.response(env, new AppendEntriesResponse(this.currentTerm, true));
    }


    synchronized IndexTerm submit0(byte [] command) {
        if (this.role != Role.LEADER) {
            return INVALID_INDEX_TERM;
        }
        long index = this.raftLog.appendNew(this.currentTerm, command, false);
        this.replicator.enqueueAppendEntriesToAllPeers();
        return new IndexTerm(index, this.currentTerm);
    }


    private void submitNoopEntry() {
        this.raftLog.appendNew(this.currentTerm, new byte[0], true);
        this.replicator.enqueueAppendEntriesToAllPeers();
    }

    private void performSendAppendEntries(RpcEnvelope<AppendEntriesRequest> reqEnv) {
        RaftNodeReplicator replicator;
        synchronized (this) {
            replicator = this.replicator;
        }
        if (replicator == null) { // not a leader anymore
            return;
        }

        try {
            log.debug("sending appendEntries {}", reqEnv);
            long dispatchedAt = System.nanoTime();
            RpcEnvelope<AppendEntriesResponse> respEnv = rpcClient.appendEntries(reqEnv);
            long respTerm = respEnv.payload().term();
            if (respTerm > this.currentTerm) {
                becomeFollower(respTerm, null);
                return;
            }
            replicator.appendReqSuccess(reqEnv, respEnv, dispatchedAt);
            maybeAdvanceCommitIndex();
        } catch (RaftRpcException e) {
            replicator.appendReqFailure(reqEnv);
            log.debug("failed to send AppendEntries to {}: {}", reqEnv.recipient(), e.getLocalizedMessage());
        } catch (Exception e) {
            replicator.appendReqFailure(reqEnv);
            log.error("Exception while sending AppendEntries to {}", reqEnv.recipient(), e);
        }
    }

    synchronized void maybeAdvanceCommitIndex() {
        if (this.role != Role.LEADER || this.replicator == null) {
            return;
        }

        long majorityIndex = this.replicator.majorityMatchIndex(this.raftLog.lastIndex());
        if (majorityIndex > this.commitIndex && this.raftLog.termAt(majorityIndex) == this.currentTerm) {
            log.debug("advancing commitIndex from: {} to: {}", this.commitIndex, majorityIndex);
            this.commitIndex = majorityIndex;
            applyCommitedEntries();
        }
    }

    private synchronized void applyCommitedEntries() {
        for (long i = this.lastApplied + 1; i <= this.commitIndex; i++) {
            LogEntry entry = this.raftLog.entryAt(i);
            IndexTerm indexTerm = new IndexTerm(entry.index(), entry.term());
            byte[] applyResult = null;
            if (!entry.noop()) {
                applyResult = this.stateMachine.apply(entry.command());
            }
            FutureResponse fr = this.futuresMap.get(indexTerm);
            log.debug("Applying entry index={} term={} reqId={}", entry.index(), entry.term(), fr != null ? fr.requestId() : null);
            if (fr != null) {
                fr.future.complete(RaftResponse.success(fr.requestId(), this.nodeId, applyResult));
            }
            this.lastApplied = i;
        }
    }

    private void performElection() {
        try {
            if (this.role == Role.LEADER) {
                return;
            }
            log.info("Starting new election, voting for myself..");
            final long electionTerm;
            final RequestVoteRequest voteRequest;
            synchronized (this) {
                this.currentTerm += 1;
                electionTerm = this.currentTerm;
                this.role = Role.CANDIDATE;
                this.votedFor = this.nodeId;
                voteRequest = new RequestVoteRequest(electionTerm, raftLog.lastIndex(), raftLog.lastTerm());
            }
            var completionService = new ExecutorCompletionService<RpcEnvelope<RequestVoteResponse>>(this.taskExecutor);

            for (String peerId : this.peerIds) {
                completionService.submit(() -> {
                    RpcEnvelope<RequestVoteResponse> env = rpcClient.requestVote(this.envelopeFactory.create(this.nodeId, peerId, voteRequest));
                    log.debug("Received vote response {} ", env);
                    return env;
                });
            }
            int votes = 1; // node always votes for itself
            int majority = majority();

            if (votes < majority) {
                for (int i = 0; i < peerIds.size(); i++) {
                    try {
                        Future<RpcEnvelope<RequestVoteResponse>> future = completionService.take();
                        RpcEnvelope<RequestVoteResponse> envelope = future.get();
                        RequestVoteResponse voteResponse = envelope.payload();
                        if (voteResponse.voteGranted()) {
                            votes += 1;
                        }

                        if (votes >= majority) {
                            this.becomeLeader(electionTerm);
                            break; //do not need to pay attention to any more voting results
                        }

                    } catch (InterruptedException e) {
                        log.debug("Interrupted while waiting for vote responses", e);
                        Thread.currentThread().interrupt();
                        break;
                    } catch (ExecutionException e) {
                        log.debug("Execution exception while waiting for vote responses", e);
                        // something wrong with one of the peers, keep going, as majority might be still possible
                    }
                }
                if (this.role == Role.CANDIDATE && this.currentTerm == electionTerm) {
                    log.info("Election for term {} did not win: {}/{} votes (needed {})", electionTerm, votes, peerIds.size() + 1, majority);
                }
            } else { // when cluster is a single node
                this.becomeLeader(electionTerm);
            }

        } finally {
            this.electionScheduler.resetElectionTimer();
        }
    }

    private synchronized void becomeLeader(long electionTerm) {
        if (role != Role.CANDIDATE || this.currentTerm != electionTerm) {
            return;
        }
        log.info("Becoming leader at the current term {}", this.currentTerm);
        this.role = Role.LEADER;
        this.leaderId = this.nodeId;
        long lastAckedReqId = envelopeFactory.lastUsedReqId();
        this.replicator = new RaftNodeReplicator(this.peerIds, this, this.raftLog, this.taskExecutor, this.scheduledExecutor, lastAckedReqId);
        this.replicator.start();
        this.submitNoopEntry();
    }

    synchronized void resolveReadonlyReads(long majorityConfirmedAt) {
        if (this.raftLog.termAt(this.commitIndex) != this.currentTerm || this.lastApplied != this.commitIndex) {
            return; // haven't established commitment in our own term yet -- unsafe to answer any lease read
        }
        while (true) {
            ReadonlyFuture peek = this.readonlyFutures.peek();
            if (peek == null || peek.submittedAt > majorityConfirmedAt) {
                break;
            }
            ReadonlyFuture poll = this.readonlyFutures.poll();
            CompletableFuture<RaftResponse> future = poll.future;
            byte[] data = this.stateMachine.apply(poll.request);
            log.debug("Resolving readonly reqId={} (submittedAt={}, majorityConfirmedAt={})", poll.requestId, poll.submittedAt, majorityConfirmedAt);
            future.complete(RaftResponse.success(poll.requestId, this.nodeId, data));
        }
    }

    void majorityConfirmedAt(long time) {
        this.resolveReadonlyReads(time);
    }


    private synchronized void becomeFollower(long newTerm, String leaderId) {
        //only candidate can become the follower if newTerm matches currentTerm
        if (this.currentTerm == newTerm && this.role != Role.CANDIDATE || this.currentTerm > newTerm) {
            return;
        }

        this.role = Role.FOLLOWER;
        this.leaderId = leaderId;

        if (this.replicator != null) {
            this.replicator.stop();
            this.replicator = null;
        }

        // clean up read only futures with not a leader error
        ReadonlyFuture pending;
        while ((pending = this.readonlyFutures.poll()) != null) {
            pending.future.complete(RaftResponse.notLeaderError(pending.requestId, this.nodeId, this.leaderId));
        }
        // clean up write futures with timeout error
        // because it is possible that corresponding log entries already replicated and can end up
        // in a new leader's commit index, so it's safer to respond with indefinite error (timeout)
        for (FutureResponse fr : this.futuresMap.values()) {
            fr.future.complete(RaftResponse.timeoutError(fr.requestId(), this.nodeId));
        }

        if (newTerm > this.currentTerm) {
            log.info("Becoming follower with new term, new term: {}, prev term: {}, leader: {}", newTerm, this.currentTerm, leaderId);
            this.votedFor = null;
            this.currentTerm = newTerm;
        } else {
            log.info("Becoming follower within the same term: {}, leader: {}", this.currentTerm, leaderId);
        }
    }

    public synchronized void start() {
        log.info("Starting, term={}, peers={}", this.currentTerm, this.peerIds);
        this.running = true;
        this.electionScheduler.start();
    }

    public synchronized void stop() {
        log.info("Stopping, term={}, role={}, commitIndex={}", this.currentTerm, this.role, this.commitIndex);
        this.running = false;
        this.electionScheduler.stop();
        if (this.replicator != null) {
            this.replicator.stop();
        }
        this.taskExecutor.shutdown();
        this.scheduledExecutor.shutdown();
    }

    private int majority() {
        return (peerIds.size() + 1) / 2 + 1;
    }

    synchronized String getVotedFor0() {
        return this.votedFor;
    }

    public synchronized Optional<LogEntry> getEntryAt0(long index) {
        if (index < 1 || index > this.raftLog.lastIndex()) {
            return Optional.empty();
        }
        return Optional.of(this.raftLog.entryAt(index));
    }


    public synchronized Optional<Map<String, PeerReplicationStatus>> getReplicationStatus() {
        if (this.replicator == null) {
            return Optional.empty();
        }
        ReplicationState state = this.replicator.getReplicationState();
        Map<String, PeerReplicationStatus> statuses = new HashMap<>();
        for (String peerId : this.peerIds) {
            ReplicationState.PeerProgress progress = state.getPeerProgress(peerId);
            statuses.put(peerId, new PeerReplicationStatus(progress.matchIndex(), progress.nextIndex(), progress.lastConfirmedDispatchAt()));
        }
        return Optional.of(statuses);
    }


    public boolean isRunning() {
        return running;
    }

    public Role getRole() {
        return role;
    }

    public long getCurrentTerm() {
        return this.currentTerm;
    }

    public long getCommitIndex() {
        return this.commitIndex;
    }

    public String getId() {
        return this.nodeId;
    }

    public List<String> getPeerIds() {
        return peerIds;
    }

    public String getLeaderId() {
        return this.leaderId;
    }
}