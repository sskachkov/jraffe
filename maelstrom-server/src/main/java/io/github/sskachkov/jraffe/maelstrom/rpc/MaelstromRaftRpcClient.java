package io.github.sskachkov.jraffe.maelstrom.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcClient;
import io.github.sskachkov.jraffe.core.rpc.RaftRpcException;

import java.util.Base64;
import java.util.concurrent.ExecutionException;

// Peer RPC over Maelstrom, built on MaelstromTransport. Correlation, msg_id assignment,
// and the actual stdout write all live in the transport - this class only builds/parses
// the Raft-specific message bodies and blocks on the transport's reply future.
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
    public RequestVoteResponse requestVote(String peerId, RequestVoteRequest request) throws RaftRpcException {
        ObjectNode body = mapper.createObjectNode();
        body.put("type", "request_vote");
        body.put("term", request.term());
        body.put("candidate_id", request.candidateId());
        body.put("last_log_index", request.lastLogIndex());
        body.put("last_log_term", request.lastLogTerm());

        JsonNode reply = await(transport.request(peerId, body, VOTE_TIMEOUT), peerId, "request_vote");
        return new RequestVoteResponse(reply.get("term").asLong(), reply.get("vote_granted").asBoolean());
    }

    @Override
    public AppendEntriesResponse appendEntries(String peerId, AppendEntriesRequest request) throws RaftRpcException {
        ObjectNode body = mapper.createObjectNode();
        body.put("type", "append_entries");
        body.put("req_id", request.reqId());
        body.put("term", request.term());
        body.put("leader_id", request.leaderId());
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

        JsonNode reply = await(transport.request(peerId, body, APPEND_TIMEOUT), peerId, "append_entries");
        return new AppendEntriesResponse(reply.get("req_id").asLong(), reply.get("term").asLong(), reply.get("success").asBoolean());
    }

    private static JsonNode await(java.util.concurrent.CompletableFuture<JsonNode> future, String peerId, String type) throws RaftRpcException {
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
