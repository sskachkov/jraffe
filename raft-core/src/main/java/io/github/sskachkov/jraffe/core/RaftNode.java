package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.message.RaftResponse;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcException;
import io.github.sskachkov.jraffe.core.rpc.RpcResponse;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A single Raft node's full state machine: role/term/log bookkeeping, leader election,
 * replication triggers, and resolution of pending client submissions. Holds the RaftLog and
 * drives an internal RaftNodeReplicator for outgoing replication; transport-agnostic &mdash; talks
 * to peers only through the injected RaftRpcClient.
 */
public class RaftNode {
    private static final TermIndex INVALID_TERM_INDEX = new TermIndex(-1, -1);
    record TermIndex(long term, long index) {}
    record FutureResponse(long requestId, CompletableFuture<RaftResponse> future) {}
    record ReadonlyFuture(long requestId, byte[] request, CompletableFuture<RaftResponse> future, long submittedAt) {}
    private final static long REQUEST_TIMEOUT = 1;
    private final Logger log;

    private final AtomicLong reqIdCounter;
    private final Map<TermIndex, FutureResponse> futuresMap;
    private final Queue<ReadonlyFuture> readonlyFutures;
    private final String nodeId;
    private final List<String> peerIds;
    private final StateMachine stateMachine;
    private final RaftRpcClient rpcClient;
    private final RaftLog raftLog;
    private final RaftNodeReplicator replicator;

    private volatile boolean running;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private volatile Role role = Role.FOLLOWER;
    private volatile long commitIndex;
    private volatile long lastApplied;
    private volatile String leaderId;
    private volatile ReplicationState replicationState;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService taskExecutor;
    private volatile ScheduledFuture<?> electionSchedule;

    public RaftNode(String nodeId, List<String> peerIds, RaftRpcClient rpcClient, StateMachine stateMachine, MeterRegistry registry) {
        this.log = new ContextAwareLogger(RaftNode.class, nodeId);
        this.reqIdCounter = new AtomicLong();
        this.futuresMap = new ConcurrentHashMap<>();
        this.readonlyFutures = new ConcurrentLinkedQueue<>();
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(4);

        this.nodeId = nodeId;
        this.peerIds = peerIds;
        this.stateMachine = stateMachine;
        this.rpcClient = rpcClient;
        this.raftLog = new RaftLog();
        this.replicator = new RaftNodeReplicator(nodeId, peerIds, rpcClient, this, this.raftLog, registry);
        scheduler.setRemoveOnCancelPolicy(true);
        this.scheduledExecutorService = ExecutorServiceMetrics.monitor(registry, scheduler, "raft-scheduler");
        this.taskExecutor = ExecutorServiceMetrics.monitor(registry, Executors.newVirtualThreadPerTaskExecutor(), "raft-taskExecutor");
    }

    public synchronized CompletableFuture<RaftResponse> submit(RaftMessage request) {
        TermIndex termIndex = submit0(request.data());
        if (termIndex.equals(INVALID_TERM_INDEX)) {
            log.debug("Rejecting submit reqId={}: not leader (term={}, leaderId={})", request.id(), this.currentTerm, this.leaderId);
            return CompletableFuture.completedFuture(RaftResponse.notLeaderError(request.id(), this.nodeId, this.leaderId));
        }
        log.debug("Accepted submit reqId={} at index={} term={}", request.id(), termIndex.index, termIndex.term);
        CompletableFuture<RaftResponse> future = new CompletableFuture<>();
        future.completeOnTimeout(RaftResponse.timeoutError(request.id(), this.nodeId), REQUEST_TIMEOUT, TimeUnit.SECONDS);
        future.whenComplete((RaftResponse rr, Throwable t) -> {
            this.futuresMap.remove(termIndex);
        });
        this.futuresMap.put(termIndex, new FutureResponse(request.id(), future));
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

    synchronized TermIndex submit0(byte [] command) {
        if (this.role != Role.LEADER) {
            return INVALID_TERM_INDEX;
        }
        long index = this.raftLog.appendNew(this.currentTerm, command, false);
        this.replicator.requestSendAppendEntriesToAllPeers();
        return new TermIndex(this.currentTerm, index);
    }

    private synchronized void submitNoopEntry() {
        this.raftLog.appendNew(this.currentTerm, new byte[0], true);
        this.replicator.requestSendAppendEntriesToAllPeers();
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

    private void beginElection() {
        if (!this.running) {
            return;
        }
        if (this.role == Role.LEADER) {
            this.rescheduleElection();
            return;
        }
        log.info("Starting leader election, voting for myself.");
        RaftRpcClient.RequestVoteRequest req;
        long electionTerm;
        synchronized (this) {
            this.currentTerm += 1;
            electionTerm = this.currentTerm;
            this.role = Role.CANDIDATE;
            this.replicationState = null;
            this.votedFor = this.nodeId;
            req = new RaftRpcClient.RequestVoteRequest(this.currentTerm, this.nodeId, raftLog.lastIndex(), raftLog.lastTerm());
        }

        var completionService = new ExecutorCompletionService<RpcResponse<RaftRpcClient.RequestVoteResponse>>(this.taskExecutor);

        for (String peerId : peerIds) {
            completionService.submit(() -> {
                try {
                    log.debug("Sending request vote to {}..", peerId);
                    RaftRpcClient.RequestVoteResponse resp = rpcClient.requestVote(peerId, req);
                    return new RpcResponse<>(peerId, resp);
                } catch (RaftRpcException e) {
                    log.debug("failed to send requestVote to {}: {}", peerId, e.getLocalizedMessage());
                    throw e;
                }
            });
        }
        int votes = 1;
        int majority = calcMajority();
        if (votes >= majority) {
            log.info("Received majority votes, becoming a leader.");
            becomeLeader(electionTerm);
        }
        for (int i = 0; i < peerIds.size(); i++) {
            try {
                var future = completionService.take(); // blocks until any task finishes
                RpcResponse<RaftRpcClient.RequestVoteResponse> resp = future.get();
                RaftRpcClient.RequestVoteResponse voteResp = resp.response();
                log.debug("Received vote response from {}: vote {}.", resp.peerId(), voteResp.voteGranted() ? "granted" : "denied");
                if (voteResp.voteGranted()) votes++;
                if (votes >= majority) {
                    log.info("Received majority votes, becoming a leader.");
                    becomeLeader(electionTerm);
                    break; // we collected enough answers
                }
            } catch (ExecutionException e) {
                // peer unreachable — doesn't count, keep going
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        this.rescheduleElection();
    }

    int calcMajority() {
        return (peerIds.size() + 1) / 2 + 1;
    }

    private synchronized void becomeLeader(long electionTerm) {
        if (role != Role.CANDIDATE || this.currentTerm != electionTerm) {
            return;
        }
        this.role = Role.LEADER;
        this.leaderId = this.nodeId;
        this.replicationState = new ReplicationState(reqIdCounter, peerIds, this.raftLog.lastIndex());
        this.submitNoopEntry();
    }


    // called by the replicator once a peer's AppendEntries RPC completes -- decides what the response
    // means (step down, record progress, advance commit, release reads), so the replicator itself only
    // has to decide when/what to send, never how to interpret a reply.
    synchronized void onAppendEntriesResponse(String peerId, RaftRpcClient.AppendEntriesResponse resp,
                                               long prevLogIndex, int entriesSent, long dispatchedAt) {
        if (resp.term() > this.currentTerm) {
            log.info("Peer {} responded with higher term {} (was {}), stepping down", peerId, resp.term(), this.currentTerm);
            becomeFollower(resp.term());
            return;
        }
        if (this.replicationState == null) {
            return; // no longer leader, ignore
        }
        if (resp.success()) {
            this.replicationState.recordSuccess(peerId, resp.reqId(), prevLogIndex + entriesSent, dispatchedAt);
            maybeAdvanceCommitIndex();
        } else {
            this.replicationState.recordFailure(peerId, resp.reqId(), dispatchedAt);
        }
        long majorityConfirmedAt = this.replicationState.majorityConfirmedDispatchAt(System.nanoTime());
        resolveReadonlyReads(majorityConfirmedAt);
    }

    synchronized void maybeAdvanceCommitIndex() {
        if (this.role != Role.LEADER || this.replicationState == null) {
            return;
        }

        long majorityIndex = this.replicationState.majorityMatchIndex(this.raftLog.lastIndex());
        if (majorityIndex > this.commitIndex && this.raftLog.termAt(majorityIndex) == this.currentTerm) {
            this.commitIndex = majorityIndex;
            applyCommittedEntries();
        }
    }

    private synchronized void applyCommittedEntries() {
        for (long i = this.lastApplied + 1; i <= this.commitIndex; i++) {
            LogEntry entry = this.raftLog.entryAt(i);
            TermIndex termIndex = new TermIndex(entry.term(), entry.index());
            byte[] applyResult = null;
            if (!entry.noop()) {
                applyResult = this.stateMachine.apply(entry.command());
            }
            FutureResponse fr = this.futuresMap.get(termIndex);
            log.debug("Applying entry index={} term={} reqId={}", entry.index(), entry.term(), fr != null ? fr.requestId() : null);
            if (fr != null) {
                fr.future.complete(RaftResponse.success(fr.requestId(), this.nodeId, applyResult));
            }
            this.lastApplied = i;
        }
    }

    private synchronized void rescheduleElection() {
        if (!this.running) {
            return;
        }
        if (this.electionSchedule != null) {
            this.electionSchedule.cancel(false);
        }
        int delay = ThreadLocalRandom.current().nextInt(500, 1000);
        this.electionSchedule = scheduledExecutorService.schedule(() -> taskExecutor.execute(this::beginElection), delay, TimeUnit.MILLISECONDS);
    }


    // must run with `this` locked -- caller is stepping down and leaderId/role are mid-transition
    private void failPendingSubmissions() {
        ReadonlyFuture pendingRead;
        while ((pendingRead = this.readonlyFutures.poll()) != null) {
            CompletableFuture<RaftResponse> f = pendingRead.future;
            f.complete(RaftResponse.notLeaderError(pendingRead.requestId, this.nodeId, null));
        }
    }

    synchronized void becomeFollower(long newTerm) {
        if ((this.role != Role.CANDIDATE && newTerm <= this.currentTerm) ||
                (this.role == Role.CANDIDATE && newTerm < this.currentTerm)) {
            return;
        }
        this.role = Role.FOLLOWER;
        this.replicationState = null;
        failPendingSubmissions();
        if (newTerm > this.currentTerm) {
            log.info("Becoming a follower with the term: {}, prev term was: {} ", newTerm, this.currentTerm);
            this.votedFor = null;
            this.leaderId = null;
            this.currentTerm = newTerm;
        } else {
            log.info("Becoming a follower within the same term: {}", this.currentTerm);
        }
    }

    synchronized Optional<LogEntry> getEntryAt0(long index) {
        if (index < 1 || index > this.raftLog.lastIndex()) {
            return Optional.empty();
        }
        return Optional.of(this.raftLog.entryAt(index));
    }

    public long getCommitIndex() {
        return this.commitIndex;
    }

    String getVotedFor0() {
        return this.votedFor;
    }

    public synchronized RaftRpcClient.RequestVoteResponse handleRequestVote(RaftRpcClient.RequestVoteRequest req) {
        if (req.term() > this.currentTerm) {
            becomeFollower(req.term());
        }
        boolean grantVote = req.term() == this.currentTerm && (this.votedFor == null || this.votedFor.equals(req.candidateId()));
        grantVote = grantVote && this.raftLog.isCandidateUpToDate(req.lastLogIndex(), req.lastLogTerm());
        log.debug("received vote request for the term {} and candidate {}, responding with {}.",
                req.term(), req.candidateId(), grantVote);
        if (grantVote) {
            this.votedFor = req.candidateId();
            rescheduleElection();
        }
        return new RaftRpcClient.RequestVoteResponse(this.currentTerm, grantVote);
    }

    public synchronized RaftRpcClient.AppendEntriesResponse handleAppendEntries(RaftRpcClient.AppendEntriesRequest req) {
        if (req.term() < this.currentTerm) {
            return new RaftRpcClient.AppendEntriesResponse(req.reqId(), this.currentTerm, false);
        }
        if (req.term() > this.currentTerm || this.role == Role.CANDIDATE) {
            becomeFollower(req.term());
        }
        this.leaderId = req.leaderId();

        rescheduleElection();
        boolean appendSuccessful = raftLog.tryAppend(req.prevLogIndex(), req.prevLogTerm(), req.entries());
        if (!appendSuccessful) {
            return new RaftRpcClient.AppendEntriesResponse(req.reqId(), this.currentTerm, false);
        }
        if (this.commitIndex < req.leaderCommit()) {
            //min would only make sense if we add limit to a size of newEntries within single appendEntries req
            this.commitIndex = Math.min(req.leaderCommit(), raftLog.lastIndex());
            applyCommittedEntries();
        }
        return new RaftRpcClient.AppendEntriesResponse(req.reqId(), this.currentTerm, true);
    }

    public String getNodeId() {
        return nodeId;
    }

    public Role getRole() {
        return role;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void start() {
        this.running = true;
        this.replicator.start();
        this.rescheduleElection();
    }

    public synchronized void shutdown() {
        this.running = false;
        this.electionSchedule.cancel(false);
        this.replicator.shutdown();
        this.scheduledExecutorService.shutdown();
        this.taskExecutor.shutdown();
    }

    public boolean isRunning() {
        return this.running;
    }

    public ReplicationState getReplicationState() {
        return replicationState;
    }


    public List<String> getPeerIds() {
        return this.peerIds;
    }

    public String getLeaderId() {
        return this.leaderId;
    }

}