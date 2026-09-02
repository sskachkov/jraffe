package io.github.sskachkov.jraffe.server.rpc;

import com.google.protobuf.ByteString;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesRequest;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesResponse;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteRequest;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteResponse;
import io.github.sskachkov.jraffe.core.rpc.RpcEnvelope;
import io.github.sskachkov.jraffe.server.FakeRaftNode;
import io.github.sskachkov.jraffe.server.grpc.proto.RaftProto;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RaftGrpcServiceTest {

    private static class CapturingObserver implements StreamObserver<RaftProto.RpcEnvelope> {
        RaftProto.RpcEnvelope value;
        Throwable error;
        boolean completed;

        @Override public void onNext(RaftProto.RpcEnvelope v) { this.value = v; }
        @Override public void onError(Throwable t) { this.error = t; }
        @Override public void onCompleted() { this.completed = true; }
    }

    @Test
    void requestVoteConvertsRequestAndReturnsRealEnvelopeFields() {
        FakeRaftNode fake = new FakeRaftNode();
        AtomicReference<RpcEnvelope<RequestVoteRequest>> captured = new AtomicReference<>();
        fake.onRequestVote = reqEnv -> {
            captured.set(reqEnv);
            return new RpcEnvelope<>("n1", "n2", 42L, 9L, 7L, new RequestVoteResponse(3, true));
        };
        RaftGrpcService service = new RaftGrpcService(fake, "n1");

        RaftProto.RpcEnvelope grpcReq = RaftProto.RpcEnvelope.newBuilder()
                .setSender("n2").setRecipient("n1").setSentAt(1L).setRequestId(2L).setCorrelationId(-1L)
                .setRequestVoteRequest(RaftProto.RequestVoteRequest.newBuilder()
                        .setTerm(3).setLastLogIndex(5).setLastLogTerm(2).build())
                .build();

        CapturingObserver observer = new CapturingObserver();
        service.requestVote(grpcReq, observer);

        // incoming request was decoded into the correct domain envelope + payload
        assertEquals("n2", captured.get().sender());
        assertEquals("n1", captured.get().recipient());
        assertEquals(3, captured.get().payload().term());
        assertEquals(5, captured.get().payload().lastLogIndex());
        assertEquals(2, captured.get().payload().lastLogTerm());

        // reply envelope reflects the REAL response envelope RaftNode returned, not builder defaults
        assertNotNull(observer.value);
        assertEquals("n1", observer.value.getSender());
        assertEquals("n2", observer.value.getRecipient());
        assertEquals(42L, observer.value.getSentAt());
        assertEquals(9L, observer.value.getRequestId());
        assertEquals(7L, observer.value.getCorrelationId());
        assertEquals(3L, observer.value.getRequestVoteResponse().getTerm());
        assertTrue(observer.value.getRequestVoteResponse().getVoteGranted());
        assertTrue(observer.completed);
        assertNull(observer.error);
    }

    @Test
    void appendEntriesConvertsRequestWithEntriesAndReturnsRealEnvelopeFields() {
        FakeRaftNode fake = new FakeRaftNode();
        AtomicReference<RpcEnvelope<AppendEntriesRequest>> captured = new AtomicReference<>();
        fake.onAppendEntries = reqEnv -> {
            captured.set(reqEnv);
            return new RpcEnvelope<>("n1", "n2", 55L, 11L, 6L, new AppendEntriesResponse(3, false));
        };
        RaftGrpcService service = new RaftGrpcService(fake, "n1");

        RaftProto.LogEntry grpcEntry = RaftProto.LogEntry.newBuilder()
                .setIndex(4).setTerm(2)
                .setCommand(ByteString.copyFrom("cmd".getBytes(StandardCharsets.UTF_8)))
                .setNoop(false)
                .build();
        RaftProto.RpcEnvelope grpcReq = RaftProto.RpcEnvelope.newBuilder()
                .setSender("n2").setRecipient("n1").setSentAt(1L).setRequestId(2L).setCorrelationId(-1L)
                .setAppendEntriesRequest(RaftProto.AppendEntriesRequest.newBuilder()
                        .setTerm(3).setPrevLogIndex(3).setPrevLogTerm(2).setLeaderCommit(3)
                        .addEntries(grpcEntry).build())
                .build();

        CapturingObserver observer = new CapturingObserver();
        service.appendEntries(grpcReq, observer);

        AppendEntriesRequest domainReq = captured.get().payload();
        assertEquals(3, domainReq.term());
        assertEquals(3, domainReq.prevLogIndex());
        assertEquals(2, domainReq.prevLogTerm());
        assertEquals(3, domainReq.leaderCommit());
        assertEquals(1, domainReq.entries().size());
        LogEntry decodedEntry = domainReq.entries().get(0);
        assertEquals(4, decodedEntry.index());
        assertEquals(2, decodedEntry.term());
        assertArrayEquals("cmd".getBytes(StandardCharsets.UTF_8), decodedEntry.command());
        assertFalse(decodedEntry.noop());

        assertEquals(55L, observer.value.getSentAt());
        assertEquals(11L, observer.value.getRequestId());
        assertEquals(6L, observer.value.getCorrelationId());
        assertEquals(3L, observer.value.getAppendEntriesResponse().getTerm());
        assertFalse(observer.value.getAppendEntriesResponse().getSuccess());
    }
}
