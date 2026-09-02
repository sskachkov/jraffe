package io.github.sskachkov.jraffe.server.rpc;

import com.google.protobuf.ByteString;
import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.rpc.*;
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
    public RpcEnvelope<RequestVoteResponse> requestVote(RpcEnvelope<RequestVoteRequest> reqEnv) throws RaftRpcException {
        Timer.Sample start = Timer.start(this.registry);
        try {
            // network call happens here
            var grpcResp = stubs.get(reqEnv.recipient()).withDeadlineAfter(VOTE_TIMEOUT, TimeUnit.MILLISECONDS).requestVote(toGrpc(reqEnv));
            return fromGrpc(grpcResp);
        } catch (StatusRuntimeException sre) {
            log.debug("StatusRuntimeException", sre);
            throw new RaftRpcException("requestVote to " + reqEnv.recipient() + " failed", sre);
        } finally {
            start.stop(requestVoteTimer);
        }
    }

    @Override
    public RpcEnvelope<AppendEntriesResponse> appendEntries(RpcEnvelope<AppendEntriesRequest> reqEnv) throws RaftRpcException {
        Timer.Sample start = Timer.start(this.registry);
        try {
            // network call happens here
            var grpcResp = stubs.get(reqEnv.recipient()).withDeadlineAfter(APPEND_TIMEOUT, TimeUnit.MILLISECONDS).appendEntries(toGrpc(reqEnv));
            return fromGrpc(grpcResp);
        } catch (StatusRuntimeException sre) {
            log.debug("StatusRuntimeException", sre);
            throw new RaftRpcException("appendEntries to " + reqEnv.recipient() + " failed", sre);
        } finally {
            start.stop(appendEntriesTimer);
        }
    }

    RaftProto.RpcEnvelope toGrpc(RpcEnvelope envelope) {
        Object req = envelope.payload();
        RaftProto.RpcEnvelope.Builder envBuilder = RaftProto.RpcEnvelope.newBuilder();
        envBuilder.setRecipient(envelope.recipient())
                .setSender(envelope.sender())
                .setRequestId(envelope.requestId())
                .setCorrelationId(envelope.correlationId())
                .setSentAt(envelope.sentAt());
        switch (req) {
            case RequestVoteRequest r -> {
                RaftProto.RequestVoteRequest voteReq = toGrpc(r);
                envBuilder.setRequestVoteRequest(voteReq);
            }
            case AppendEntriesRequest r -> {
                RaftProto.AppendEntriesRequest appendReq = toGrpc(r);
                envBuilder.setAppendEntriesRequest(appendReq);
            }
            default -> throw new IllegalStateException("Unexpected value: " + req);
        }
        return envBuilder.build();
    }
    private static RaftProto.RequestVoteRequest toGrpc(RequestVoteRequest request) {
        return RaftProto.RequestVoteRequest.newBuilder()
                .setTerm(request.term())
                .setLastLogIndex(request.lastLogIndex())
                .setLastLogTerm(request.lastLogTerm())
                .build();
    }

    static RpcEnvelope fromGrpc(RaftProto.RpcEnvelope envelope) {
        Object payload = switch (envelope.getPayloadCase()) {
            case REQUEST_VOTE_RESPONSE -> fromGrpc(envelope.getRequestVoteResponse());
            case APPEND_ENTRIES_RESPONSE -> fromGrpc(envelope.getAppendEntriesResponse());
            default -> throw new IllegalStateException("Unexpected payload case: " + envelope.getPayloadCase());
        };
        return new RpcEnvelope<>(envelope.getSender(), envelope.getRecipient(), envelope.getSentAt(), envelope.getRequestId(), envelope.getCorrelationId(), payload);
    }

    private static RequestVoteResponse fromGrpc(RaftProto.RequestVoteResponse grpcResponse) {
        return new RequestVoteResponse(grpcResponse.getTerm(), grpcResponse.getVoteGranted());
    }

    private static RaftProto.AppendEntriesRequest toGrpc(AppendEntriesRequest request) {
        var builder = RaftProto.AppendEntriesRequest.newBuilder()
                .setTerm(request.term())
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
        return new AppendEntriesResponse(grpcResponse.getTerm(), grpcResponse.getSuccess());
    }
}