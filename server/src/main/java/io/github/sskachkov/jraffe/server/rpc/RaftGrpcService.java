package io.github.sskachkov.jraffe.server.rpc;

import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.rpc.*;
import io.github.sskachkov.jraffe.server.RaftServer;
import io.github.sskachkov.jraffe.server.grpc.proto.RaftProto;
import io.github.sskachkov.jraffe.server.grpc.proto.RaftServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.List;

public class RaftGrpcService extends RaftServiceGrpc.RaftServiceImplBase {
    private final Logger log;

    private final RaftNode raftNode;

    public RaftGrpcService(RaftNode raftNode, String nodeId) {
        this.log = new ContextAwareLogger(RaftServer.class, nodeId);
        this.raftNode = raftNode;
    }

    @Override
    public void requestVote(RaftProto.RpcEnvelope grpcReqEnv,
                            StreamObserver<RaftProto.RpcEnvelope> responseObserver) {

        RaftProto.RequestVoteRequest grpcReq = grpcReqEnv.getRequestVoteRequest();
        var domainRequest = new RequestVoteRequest(
                grpcReq.getTerm(),
                grpcReq.getLastLogIndex(),
                grpcReq.getLastLogTerm()
        );

        RpcEnvelope<RequestVoteRequest> reqEnv = new RpcEnvelope<>(grpcReqEnv.getSender(), grpcReqEnv.getRecipient(), grpcReqEnv.getSentAt(), grpcReqEnv.getRequestId(), grpcReqEnv.getCorrelationId(), domainRequest);
        RpcEnvelope<RequestVoteResponse> respEnv = raftNode.handleRequestVote(reqEnv);// the actual logic
        RequestVoteResponse resp = respEnv.payload();
        RaftProto.RequestVoteResponse.Builder grpcResp = RaftProto.RequestVoteResponse.newBuilder();
        grpcResp.setTerm(resp.term())
                .setVoteGranted(resp.voteGranted());

        RaftProto.RpcEnvelope.Builder grpcRespEnv = RaftProto.RpcEnvelope.newBuilder();
        grpcRespEnv.setSender(respEnv.sender())
                .setRecipient(respEnv.recipient())
                .setSentAt(respEnv.sentAt())
                .setRequestId(respEnv.requestId())
                .setCorrelationId(respEnv.correlationId())
                .setRequestVoteResponse(grpcResp.build());

        responseObserver.onNext(grpcRespEnv.build());
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(RaftProto.RpcEnvelope grpcReqEnv,
                              StreamObserver<RaftProto.RpcEnvelope> responseObserver) {
        RaftProto.AppendEntriesRequest grpcReq = grpcReqEnv.getAppendEntriesRequest();
        List<LogEntry> entries = grpcReq.getEntriesList().stream()
                .map(e -> new LogEntry(e.getIndex(), e.getTerm(), e.getCommand().toByteArray(), e.getNoop()))
                .toList();

        var domainRequest = new AppendEntriesRequest(
                grpcReq.getTerm(),
                grpcReq.getPrevLogIndex(),
                grpcReq.getPrevLogTerm(),
                entries,
                grpcReq.getLeaderCommit()
        );
        RpcEnvelope<AppendEntriesRequest> reqEnv = new RpcEnvelope<>(grpcReqEnv.getSender(),
                grpcReqEnv.getRecipient(),
                grpcReqEnv.getSentAt(),
                grpcReqEnv.getRequestId(),
                grpcReqEnv.getCorrelationId(),
                domainRequest);

        RpcEnvelope<AppendEntriesResponse> respEnv = raftNode.handleAppendEntries(reqEnv);
        AppendEntriesResponse resp = respEnv.payload();

        RaftProto.AppendEntriesResponse.Builder appendEntriesResp = RaftProto.AppendEntriesResponse.newBuilder();
        appendEntriesResp
                .setTerm(resp.term())
                .setSuccess(resp.success())
                .build();

        RaftProto.RpcEnvelope.Builder grpcEnv = RaftProto.RpcEnvelope.newBuilder();
        grpcEnv.setSender(respEnv.sender())
                .setRecipient(respEnv.recipient())
                .setSentAt(respEnv.sentAt())
                .setRequestId(respEnv.requestId())
                .setCorrelationId(respEnv.correlationId())
                .setAppendEntriesResponse(appendEntriesResp.build());

        responseObserver.onNext(grpcEnv.build());
        responseObserver.onCompleted();
    }
}