package io.github.sskachkov.jraffe.maelstrom.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesRequest;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesResponse;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcException;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteRequest;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteResponse;
import io.github.sskachkov.jraffe.core.rpc.RpcEnvelope;

import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

// Peer RPC over Maelstrom, built on MaelstromTransport. Correlation, msg_id assignment,
// and the actual stdout write all live in the transport - this class only builds/parses
// the Raft-specific message bodies and blocks on the transport's reply future.
//
// Envelope metadata (sender/recipient/sentAt/requestId/correlationId) travels on the wire
// exactly like it does over gRPC (see GrpcRaftRpcClient) - it is never fabricated locally.
// sender/recipient duplicate Maelstrom's own src/dest, but keeping them explicit here mirrors
// the gRPC wire format and keeps this client from having to special-case its own envelope
// construction.
public class MaelstromRaftRpcClient implements RaftRpcClient {
    public static final long VOTE_TIMEOUT = 150;
    public static final long APPEND_TIMEOUT = 150;

    private final MaelstromTransport transport;
    private final ObjectMapper mapper;

    public MaelstromRaftRpcClient(MaelstromTransport transport, ObjectMapper mapper) {
        this.transport = transport;
        this.mapper = mapper;
    }

    @Override
    public RpcEnvelope<RequestVoteResponse> requestVote(RpcEnvelope<RequestVoteRequest> reqEnv) throws RaftRpcException {
        RequestVoteRequest request = reqEnv.payload();
        ObjectNode body = mapper.createObjectNode();
        body.put("type", "request_vote");
        putEnvelope(body, reqEnv);
        body.put("term", request.term());
        body.put("last_log_index", request.lastLogIndex());
        body.put("last_log_term", request.lastLogTerm());

        JsonNode reply = await(transport.request(reqEnv.recipient(), body, VOTE_TIMEOUT), reqEnv.recipient(), "request_vote");
        RequestVoteResponse response = new RequestVoteResponse(reply.get("term").asLong(), reply.get("vote_granted").asBoolean());
        return envelopeFromReply(reply, response);
    }

    @Override
    public RpcEnvelope<AppendEntriesResponse> appendEntries(RpcEnvelope<AppendEntriesRequest> reqEnv) throws RaftRpcException {
        AppendEntriesRequest request = reqEnv.payload();
        ObjectNode body = mapper.createObjectNode();
        body.put("type", "append_entries");
        putEnvelope(body, reqEnv);
        body.put("term", request.term());
        body.put("prev_log_index", request.prevLogIndex());
        body.put("prev_log_term", request.prevLogTerm());
        body.put("leader_commit", request.leaderCommit());
        ArrayNode entries = body.putArray("entries");
        for (LogEntry entry : request.entries()) {
            ObjectNode entryNode = entries.addObject();
            entryNode.put("index", entry.index());
            entryNode.put("term", entry.term());
            entryNode.put("command", Base64.getEncoder().encodeToString(entry.command()));
            entryNode.put("noop", entry.noop());
        }

        JsonNode reply = await(transport.request(reqEnv.recipient(), body, APPEND_TIMEOUT), reqEnv.recipient(), "append_entries");
        AppendEntriesResponse response = new AppendEntriesResponse(reply.get("term").asLong(), reply.get("success").asBoolean());
        return envelopeFromReply(reply, response);
    }

    private static void putEnvelope(ObjectNode body, RpcEnvelope<?> envelope) {
        body.put("sender", envelope.sender());
        body.put("recipient", envelope.recipient());
        body.put("sent_at", envelope.sentAt());
        body.put("request_id", envelope.requestId());
        body.put("correlation_id", envelope.correlationId());
    }

    private static <T> RpcEnvelope<T> envelopeFromReply(JsonNode reply, T payload) {
        return new RpcEnvelope<>(
                reply.get("sender").asText(),
                reply.get("recipient").asText(),
                reply.get("sent_at").asLong(),
                reply.get("request_id").asLong(),
                reply.get("correlation_id").asLong(),
                payload);
    }

    private static JsonNode await(CompletableFuture<JsonNode> future, String peerId, String type) throws RaftRpcException {
        try {
            return future.get();
        } catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RaftRpcException(type + " to " + peerId + " failed", e);
        }
    }
}
