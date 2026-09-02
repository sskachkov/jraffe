package io.github.sskachkov.jraffe.server.rpc;

import io.github.sskachkov.jraffe.core.rpc.AppendEntriesRequest;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesResponse;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteRequest;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteResponse;
import io.github.sskachkov.jraffe.core.rpc.RpcEnvelope;
import io.github.sskachkov.jraffe.server.grpc.proto.RaftProto;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GrpcRaftRpcClientTest {

    private final GrpcRaftRpcClient client = new GrpcRaftRpcClient(Map.of(), "n1", new SimpleMeterRegistry());

    // -- toGrpc: envelope metadata + RequestVoteRequest payload --

    @Test
    void toGrpcConvertsEnvelopeAndRequestVoteRequestFields() {
        RpcEnvelope<RequestVoteRequest> envelope = new RpcEnvelope<>(
                "n1", "n2", 12345L, 7L, -1L, new RequestVoteRequest(3, 10, 2));

        RaftProto.RpcEnvelope grpc = client.toGrpc(envelope);

        assertEquals("n1", grpc.getSender());
        assertEquals("n2", grpc.getRecipient());
        assertEquals(12345L, grpc.getSentAt());
        assertEquals(7L, grpc.getRequestId());
        assertEquals(-1L, grpc.getCorrelationId());
        assertEquals(RaftProto.RpcEnvelope.PayloadCase.REQUEST_VOTE_REQUEST, grpc.getPayloadCase());
        assertEquals(3L, grpc.getRequestVoteRequest().getTerm());
        assertEquals(10L, grpc.getRequestVoteRequest().getLastLogIndex());
        assertEquals(2L, grpc.getRequestVoteRequest().getLastLogTerm());
    }

    // -- toGrpc: AppendEntriesRequest, including its entries list --

    @Test
    void toGrpcConvertsAppendEntriesRequestAndEntries() {
        LogEntry entry = new LogEntry(5, 2, "cmd".getBytes(StandardCharsets.UTF_8), false);
        RpcEnvelope<AppendEntriesRequest> envelope = new RpcEnvelope<>(
                "n1", "n2", 1L, 2L, -1L,
                new AppendEntriesRequest(3, 4, 2, List.of(entry), 4));

        RaftProto.RpcEnvelope grpc = client.toGrpc(envelope);

        assertEquals(RaftProto.RpcEnvelope.PayloadCase.APPEND_ENTRIES_REQUEST, grpc.getPayloadCase());
        RaftProto.AppendEntriesRequest req = grpc.getAppendEntriesRequest();
        assertEquals(3L, req.getTerm());
        assertEquals(4L, req.getPrevLogIndex());
        assertEquals(2L, req.getPrevLogTerm());
        assertEquals(4L, req.getLeaderCommit());
        assertEquals(1, req.getEntriesCount());
        RaftProto.LogEntry grpcEntry = req.getEntries(0);
        assertEquals(5L, grpcEntry.getIndex());
        assertEquals(2L, grpcEntry.getTerm());
        assertArrayEquals("cmd".getBytes(StandardCharsets.UTF_8), grpcEntry.getCommand().toByteArray());
        assertFalse(grpcEntry.getNoop());
    }

    @Test
    void toGrpcRejectsAResponsePayload() {
        RpcEnvelope<RequestVoteResponse> envelope = new RpcEnvelope<>(
                "n1", "n2", 1L, 2L, -1L, new RequestVoteResponse(3, true));
        assertThrows(IllegalStateException.class, () -> client.toGrpc(envelope));
    }

    // -- fromGrpc: envelope metadata + response payloads --

    @Test
    void fromGrpcConvertsRequestVoteResponse() {
        RaftProto.RpcEnvelope grpc = RaftProto.RpcEnvelope.newBuilder()
                .setSender("n2").setRecipient("n1").setSentAt(999L).setRequestId(5L).setCorrelationId(5L)
                .setRequestVoteResponse(RaftProto.RequestVoteResponse.newBuilder().setTerm(4).setVoteGranted(true).build())
                .build();

        RpcEnvelope<?> envelope = GrpcRaftRpcClient.fromGrpc(grpc);

        assertEquals("n2", envelope.sender());
        assertEquals("n1", envelope.recipient());
        assertEquals(999L, envelope.sentAt());
        assertEquals(5L, envelope.requestId());
        assertEquals(5L, envelope.correlationId());
        assertEquals(new RequestVoteResponse(4, true), envelope.payload());
    }

    @Test
    void fromGrpcConvertsAppendEntriesResponse() {
        RaftProto.RpcEnvelope grpc = RaftProto.RpcEnvelope.newBuilder()
                .setSender("n2").setRecipient("n1").setSentAt(1L).setRequestId(2L).setCorrelationId(2L)
                .setAppendEntriesResponse(RaftProto.AppendEntriesResponse.newBuilder().setTerm(4).setSuccess(true).build())
                .build();

        RpcEnvelope<?> envelope = GrpcRaftRpcClient.fromGrpc(grpc);

        assertEquals(new AppendEntriesResponse(4, true), envelope.payload());
    }

    @Test
    void fromGrpcRejectsAnEnvelopeWithNoPayloadSet() {
        RaftProto.RpcEnvelope grpc = RaftProto.RpcEnvelope.newBuilder()
                .setSender("n2").setRecipient("n1").setSentAt(1L).setRequestId(2L).setCorrelationId(2L)
                .build();
        assertThrows(IllegalStateException.class, () -> GrpcRaftRpcClient.fromGrpc(grpc));
    }

    @Test
    void fromGrpcRejectsARequestPayload() {
        RaftProto.RpcEnvelope grpc = RaftProto.RpcEnvelope.newBuilder()
                .setSender("n2").setRecipient("n1").setSentAt(1L).setRequestId(2L).setCorrelationId(-1L)
                .setRequestVoteRequest(RaftProto.RequestVoteRequest.newBuilder().setTerm(1).build())
                .build();
        assertThrows(IllegalStateException.class, () -> GrpcRaftRpcClient.fromGrpc(grpc));
    }
}
