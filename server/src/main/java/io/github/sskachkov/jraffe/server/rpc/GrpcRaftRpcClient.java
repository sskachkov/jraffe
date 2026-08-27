package io.github.sskachkov.jraffe.server.rpc;

import com.google.protobuf.ByteString;
import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcException;
import io.github.sskachkov.jraffe.server.HostPort;
import io.github.sskachkov.jraffe.server.grpc.proto.RaftProto;
import io.github.sskachkov.jraffe.server.grpc.proto.RaftServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class GrpcRaftRpcClient implements RaftRpcClient {
    private final Logger log;

    public static final long VOTE_TIMEOUT = 150;
    public static final long APPEND_TIMEOUT = 150;

    private final Timer requestVoteTimer;

    private final Timer appendEntriesTimer;
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> stubs;
    private final MeterRegistry registry;

    public GrpcRaftRpcClient(Map<String, HostPort> peers, String nodeId, MeterRegistry registry) {
        this.log = new ContextAwareLogger(GrpcRaftRpcClient.class, nodeId);

        this.registry = registry;
        this.requestVoteTimer = Timer.builder("raft.rpc.requestVote")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);
        this.appendEntriesTimer = Timer.builder("raft.rpc.appendEntries")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.stubs = peers.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                    ManagedChannel channel = ManagedChannelBuilder
                            .forAddress(e.getValue().host(), e.getValue().port())
                            .executor(Executors.newVirtualThreadPerTaskExecutor())
                            .usePlaintext()
                            .build();
                    return RaftServiceGrpc.newBlockingStub(channel);
                }
        ));
    }

    @Override
    public RequestVoteResponse requestVote(String peerId, RequestVoteRequest request) throws RaftRpcException {
        Timer.Sample start = Timer.start(this.registry);
        try {
            // network call happens here
            var grpcResp = stubs.get(peerId).withDeadlineAfter(VOTE_TIMEOUT, TimeUnit.MILLISECONDS).requestVote(toGrpc(request));
            return fromGrpc(grpcResp);
        } catch (StatusRuntimeException sre) {
            log.trace("StatusRuntimeException", sre);
            throw new RaftRpcException("requestVote to " + peerId + " failed", sre);
        } finally {
            start.stop(requestVoteTimer);
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(String peerId, AppendEntriesRequest request) throws RaftRpcException {
        Timer.Sample start = Timer.start(this.registry);
        try {
            // network call happens here
            var grpcResp = stubs.get(peerId).withDeadlineAfter(APPEND_TIMEOUT, TimeUnit.MILLISECONDS).appendEntries(toGrpc(request));
            return fromGrpc(grpcResp);
        } catch (StatusRuntimeException sre) {
            log.trace("StatusRuntimeException", sre);
            throw new RaftRpcException("appendEntries to " + peerId + " failed", sre);
        } finally {
            start.stop(appendEntriesTimer);
        }
    }

    private static RaftProto.RequestVoteRequest toGrpc(RequestVoteRequest request) {
        return RaftProto.RequestVoteRequest.newBuilder()
                .setTerm(request.term())
                .setCandidateId(request.candidateId())
                .setLastLogIndex(request.lastLogIndex())
                .setLastLogTerm(request.lastLogTerm())
                .build();
    }

    private static RequestVoteResponse fromGrpc(RaftProto.RequestVoteResponse grpcResponse) {
        return new RequestVoteResponse(grpcResponse.getTerm(), grpcResponse.getVoteGranted());
    }

    private static RaftProto.AppendEntriesRequest toGrpc(AppendEntriesRequest request) {
        var builder = RaftProto.AppendEntriesRequest.newBuilder()
                .setReqId(request.reqId())
                .setTerm(request.term())
                .setLeaderId(request.leaderId())
                .setPrevLogIndex(request.prevLogIndex())
                .setPrevLogTerm(request.prevLogTerm())
                .setLeaderCommit(request.leaderCommit());
        for (LogEntry entry : request.entries()) {
            builder.addEntries(toGrpc(entry));
        }
        return builder.build();
    }

    private static RaftProto.LogEntry toGrpc(LogEntry entry) {
        return RaftProto.LogEntry.newBuilder()
                .setIndex(entry.index())
                .setTerm(entry.term())
                .setCommand(ByteString.copyFrom(entry.command()))
                .setNoop(entry.noop())
                .build();
    }

    private static AppendEntriesResponse fromGrpc(RaftProto.AppendEntriesResponse grpcResponse) {
        return new AppendEntriesResponse(grpcResponse.getReqId(), grpcResponse.getTerm(), grpcResponse.getSuccess());
    }
}