package io.github.sskachkov.jraffe.server.rpc;

import io.github.sskachkov.jraffe.core.RaftNode;
import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
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
    public void requestVote(RaftProto.RequestVoteRequest grpcRequest,
                            StreamObserver<RaftProto.RequestVoteResponse> responseObserver) {

        var domainRequest = new RaftRpcClient.RequestVoteRequest(
                grpcRequest.getTerm(),
                grpcRequest.getCandidateId(),
                grpcRequest.getLastLogIndex(),
                grpcRequest.getLastLogTerm()
        );

        var domainResponse = raftNode.handleRequestVote(domainRequest);   // the actual logic

        var grpcResponse = RaftProto.RequestVoteResponse.newBuilder()
                .setTerm(domainResponse.term())
                .setVoteGranted(domainResponse.voteGranted())
                .build();

        responseObserver.onNext(grpcResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(RaftProto.AppendEntriesRequest grpcRequest,
                              StreamObserver<RaftProto.AppendEntriesResponse> responseObserver) {

        List<LogEntry> entries = grpcRequest.getEntriesList().stream()
                .map(e -> new LogEntry(e.getIndex(), e.getTerm(), e.getCommand().toByteArray(), e.getNoop()))
                .toList();

        var domainRequest = new RaftRpcClient.AppendEntriesRequest(
                grpcRequest.getReqId(),
                grpcRequest.getTerm(),
                grpcRequest.getLeaderId(),
                grpcRequest.getPrevLogIndex(),
                grpcRequest.getPrevLogTerm(),
                entries,
                grpcRequest.getLeaderCommit()
        );

        var domainResponse = raftNode.handleAppendEntries(domainRequest);

        var grpcResponse = RaftProto.AppendEntriesResponse.newBuilder()
                .setReqId(domainResponse.reqId())
                .setTerm(domainResponse.term())
                .setSuccess(domainResponse.success())
                .build();

        responseObserver.onNext(grpcResponse);
        responseObserver.onCompleted();
    }
}