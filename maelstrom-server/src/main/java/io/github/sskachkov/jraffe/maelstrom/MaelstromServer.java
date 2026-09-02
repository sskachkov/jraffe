package io.github.sskachkov.jraffe.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.node.impl.RaftNodeImpl;
import io.github.sskachkov.jraffe.core.StateMachine;
import io.github.sskachkov.jraffe.core.error.NotLeaderError;
import io.github.sskachkov.jraffe.core.error.RaftError;
import io.github.sskachkov.jraffe.core.error.TimeoutError;
import io.github.sskachkov.jraffe.core.message.RaftMessage;
import io.github.sskachkov.jraffe.core.rpc.AppendEntriesRequest;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import io.github.sskachkov.jraffe.core.rpc.RequestVoteRequest;
import io.github.sskachkov.jraffe.core.rpc.RpcEnvelope;
import io.github.sskachkov.jraffe.kvstore.CVASCommandCodec;
import io.github.sskachkov.jraffe.kvstore.CVASRequest;
import io.github.sskachkov.jraffe.kvstore.CVASResponse;
import io.github.sskachkov.jraffe.kvstore.GetCommandCodec;
import io.github.sskachkov.jraffe.kvstore.GetRequest;
import io.github.sskachkov.jraffe.kvstore.GetResponse;
import io.github.sskachkov.jraffe.kvstore.KVStoreImpl;
import io.github.sskachkov.jraffe.kvstore.SetCommandCodec;
import io.github.sskachkov.jraffe.kvstore.SetRequest;
import io.github.sskachkov.jraffe.kvstore.SetResponse;
import io.github.sskachkov.jraffe.maelstrom.rpc.MaelstromRaftRpcClient;
import io.github.sskachkov.jraffe.maelstrom.rpc.StdioMaelstromTransport;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

// Owns the stdin read loop - the process blocks in run() for its whole lifetime, feeding
// each line to the transport. All the real wiring (constructing RaftNode, registering the
// per-message-type handlers) happens lazily in handleInit, since node id/peers aren't known
// until Maelstrom's "init" message arrives.
public class MaelstromServer {
    private static final Logger log = LoggerFactory.getLogger(MaelstromServer.class);

    // Covers the leader's own submit/submitReadonly internal timeout (1s, see RaftNode) plus
    // the extra network hop, so a forward doesn't time out before the leader's own attempt would.
    private static final long FORWARD_TIMEOUT_MS = 2000;

    // Ceiling on how long handleInit waits for a known leader before sending init_ok anyway.
    // A node that's partitioned away the instant it (re)starts can never satisfy
    // getLeaderId() != null on its own - it can't win an election alone, and can't learn of
    // anyone else's win either. Left unbounded, that wait is indefinite for as long as the
    // partition lasts, and Maelstrom's own init handshake timeout (10s, confirmed from
    // maelstrom.db/init-node!) fires first, crashing that nemesis step. Comfortably under 10s.
    private static final long INIT_MAX_WAIT_MS = 4000;

    private final ObjectMapper mapper;
    private final StdioMaelstromTransport transport;
    private final BufferedReader reader;
    private RaftNode raftNode;

    public MaelstromServer(InputStream in, PrintStream out) {
        this.mapper = new ObjectMapper();
        this.transport = new StdioMaelstromTransport(mapper, out);
        this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));

        transport.onMessage("init", this::handleInit);
    }

    private void handleInit(String src, JsonNode body) {
        String nodeId = body.get("node_id").asText();
        List<String> peerIds = new ArrayList<>();
        for (JsonNode idNode : body.get("node_ids")) {
            String id = idNode.asText();
            if (!id.equals(nodeId)) {
                peerIds.add(id);
            }
        }

        transport.init(nodeId);
        MaelstromRaftRpcClient rpcClient = new MaelstromRaftRpcClient(transport, mapper);
        MeterRegistry registry = new SimpleMeterRegistry();
        StateMachine stateMachine = new KvStoreStateMachine(new KVStoreImpl());
        this.raftNode = new RaftNodeImpl(nodeId, peerIds, rpcClient, stateMachine, registry);
        this.raftNode.start();

        transport.onMessage("read", this::handleRead);
        transport.onMessage("write", this::handleWrite);
        transport.onMessage("cas", this::handleCas);
        transport.onMessage("forward", this::handleForward);
        transport.onMessage("request_vote", this::handleRequestVote);
        transport.onMessage("append_entries", this::handleAppendEntries);

        ScheduledExecutorService initPoller = Executors.newSingleThreadScheduledExecutor();
        AtomicReference<ScheduledFuture<?>> pollTask = new AtomicReference<>();
        long deadline = System.currentTimeMillis() + INIT_MAX_WAIT_MS;
        pollTask.set(initPoller.scheduleAtFixedRate(() -> {
            boolean leaderKnown = raftNode.getLeaderId() != null;
            boolean timedOut = System.currentTimeMillis() >= deadline;
            if (leaderKnown || timedOut) {
                pollTask.get().cancel(false);
                initPoller.shutdown();
                ObjectNode reply = mapper.createObjectNode();
                reply.put("type", "init_ok");
                transport.reply(src, body.get("msg_id").asLong(), reply);
            }
        }, 0, 20, TimeUnit.MILLISECONDS));
    }

    // --- client kv ops: read/write/cas -----------------------------------------------------
    // These must NOT block the calling (stdin-reading) thread: raftNode.submit/submitReadonly
    // may need to wait on append_entries_res replies from peers, which arrive on this exact
    // same stdin stream. Blocking here would deadlock the whole node.
    //
    // Each has a thin onMessage-registered entry point (allowForward=true) and a shared
    // *Internal method used both directly and from handleForward (allowForward=false, so a
    // forward never triggers a second forward - single-hop only, see forward(...) below).
    // replyMsgId is passed explicitly rather than always read off body: when called from
    // handleForward, body is the original client op (nested under "op"), but the reply has to
    // correlate with the *forward* request's own msg_id, not the original client's.

    private void handleRead(String src, JsonNode body) {
        handleReadInternal(src, body, body.get("msg_id").asLong(), true);
    }

    private void handleReadInternal(String src, JsonNode body, long replyMsgId, boolean allowForward) {
        byte[] key = jsonToBytes(body.get("key"));
        log.debug("read src={} reqId={} key={}", src, replyMsgId, body.get("key"));
        byte[] reqBytes = GetCommandCodec.encodeRequest(new GetRequest(key));

        raftNode.submitReadonly(new RaftMessage(replyMsgId, reqBytes)).whenComplete((rr, ex) -> {
            if (ex != null) {
                replyInternalError(src, replyMsgId, ex);
                return;
            }
            if (!rr.isSuccess()) {
                if (allowForward && tryForward(rr.getError(), body, src, replyMsgId)) {
                    return;
                }
                replyRaftError(src, replyMsgId, rr.getError());
                return;
            }
            GetResponse getResponse = GetCommandCodec.decodeResponse(rr.getData());
            if (!getResponse.found()) {
                replyError(src, replyMsgId, 20, "key does not exist");
                return;
            }
            ObjectNode reply = mapper.createObjectNode();
            reply.put("type", "read_ok");
            reply.set("value", bytesToJson(getResponse.value()));
            transport.reply(src, replyMsgId, reply);
        });
    }

    private void handleWrite(String src, JsonNode body) {
        handleWriteInternal(src, body, body.get("msg_id").asLong(), true);
    }

    private void handleWriteInternal(String src, JsonNode body, long replyMsgId, boolean allowForward) {
        byte[] key = jsonToBytes(body.get("key"));
        byte[] value = jsonToBytes(body.get("value"));
        log.debug("write src={} reqId={} key={} value={}", src, replyMsgId, body.get("key"), body.get("value"));
        byte[] reqBytes = SetCommandCodec.encodeRequest(new SetRequest(key, value));

        raftNode.submit(new RaftMessage(replyMsgId, reqBytes)).whenComplete((rr, ex) -> {
            if (ex != null) {
                replyInternalError(src, replyMsgId, ex);
                return;
            }
            if (!rr.isSuccess()) {
                if (allowForward && tryForward(rr.getError(), body, src, replyMsgId)) {
                    return;
                }
                replyRaftError(src, replyMsgId, rr.getError());
                return;
            }
            SetResponse ignored = SetCommandCodec.decodeResponse(rr.getData());
            ObjectNode reply = mapper.createObjectNode();
            reply.put("type", "write_ok");
            transport.reply(src, replyMsgId, reply);
        });
    }

    private void handleCas(String src, JsonNode body) {
        handleCasInternal(src, body, body.get("msg_id").asLong(), true);
    }

    private void handleCasInternal(String src, JsonNode body, long replyMsgId, boolean allowForward) {
        byte[] key = jsonToBytes(body.get("key"));
        byte[] from = jsonToBytes(body.get("from"));
        byte[] to = jsonToBytes(body.get("to"));
        log.debug("cas src={} reqId={} key={} from={} to={}", src, replyMsgId, body.get("key"), body.get("from"), body.get("to"));
        byte[] reqBytes = CVASCommandCodec.encodeRequest(new CVASRequest(key, from, to));

        raftNode.submit(new RaftMessage(replyMsgId, reqBytes)).whenComplete((rr, ex) -> {
            if (ex != null) {
                replyInternalError(src, replyMsgId, ex);
                return;
            }
            if (!rr.isSuccess()) {
                if (allowForward && tryForward(rr.getError(), body, src, replyMsgId)) {
                    return;
                }
                replyRaftError(src, replyMsgId, rr.getError());
                return;
            }
            CVASResponse cvasResponse = CVASCommandCodec.decodeResponse(rr.getData());
            switch (cvasResponse.status()) {
                case SUCCESS -> {
                    ObjectNode reply = mapper.createObjectNode();
                    reply.put("type", "cas_ok");
                    transport.reply(src, replyMsgId, reply);
                }
                case KEY_NOT_FOUND -> replyError(src, replyMsgId, 20, "key does not exist");
                case VALUE_MISMATCH -> replyError(src, replyMsgId, 22,
                        "expected " + body.get("from") + " but had " + bytesToJson(cvasResponse.actualValue()));
            }
        });
    }

    // --- transparent leader forwarding -------------------------------------------------------
    // A follower that can't serve a request forwards the original op, verbatim, to whichever
    // node it believes is leader, then relays that node's answer back to the original client -
    // which never sees a "not leader" error at all except when nobody knows the leader yet.
    // Single-hop only: handleForward always calls the *Internal methods with allowForward=false,
    // so a forward landing on a node that also isn't leader just fails normally instead of
    // forwarding again - avoids any risk of a forwarding loop during a leadership race.

    private boolean tryForward(RaftError error, JsonNode op, String originalSrc, long originalMsgId) {
        if (!(error instanceof NotLeaderError nle) || nle.getSuggestedLeader() == null) {
            return false;
        }
        ObjectNode forwardBody = mapper.createObjectNode();
        forwardBody.put("type", "forward");
        forwardBody.set("op", op);
        transport.request(nle.getSuggestedLeader(), forwardBody, FORWARD_TIMEOUT_MS).whenComplete((leaderReply, ex) -> {
            if (ex instanceof TimeoutException) {
                log.debug("Forward to {} timed out for src={} msgId={}", nle.getSuggestedLeader(), originalSrc, originalMsgId);
                replyError(originalSrc, originalMsgId, 0, "timeout");
            } else if (ex != null) {
                replyInternalError(originalSrc, originalMsgId, ex);
            } else {
                transport.reply(originalSrc, originalMsgId, (ObjectNode) leaderReply);
            }
        });
        return true;
    }

    private void handleForward(String src, JsonNode body) {
        long forwardMsgId = body.get("msg_id").asLong();
        JsonNode op = body.get("op");
        String opType = op.has("type") ? op.get("type").asText() : "";
        switch (opType) {
            case "read" -> handleReadInternal(src, op, forwardMsgId, false);
            case "write" -> handleWriteInternal(src, op, forwardMsgId, false);
            case "cas" -> handleCasInternal(src, op, forwardMsgId, false);
            default -> replyError(src, forwardMsgId, 12, "cannot forward op of type: " + opType);
        }
    }

    // --- peer rpc: request_vote/append_entries ----------------------------------------------
    // handleRequestVote/handleAppendEntries on RaftNode only touch local state - safe to call
    // directly on the stdin-reading thread, unlike the client ops above.

    private void handleRequestVote(String src, JsonNode body) {
        var req = new RequestVoteRequest(
                body.get("term").asLong(),
                body.get("last_log_index").asLong(),
                body.get("last_log_term").asLong());
        RpcEnvelope<RequestVoteRequest> reqEnv = envelopeFromBody(body, req);
        var respEnv = raftNode.handleRequestVote(reqEnv);
        var resp = respEnv.payload();

        ObjectNode reply = mapper.createObjectNode();
        reply.put("type", "request_vote_res");
        putEnvelope(reply, respEnv);
        reply.put("term", resp.term());
        reply.put("vote_granted", resp.voteGranted());
        transport.reply(src, body.get("msg_id").asLong(), reply);
    }

    private void handleAppendEntries(String src, JsonNode body) {
        List<LogEntry> entries = new ArrayList<>();
        for (JsonNode entryNode : body.get("entries")) {
            LogEntry logEntry = new LogEntry(
                    entryNode.get("index").asLong(),
                    entryNode.get("term").asLong(),
                    Base64.getDecoder().decode(entryNode.get("command").asText()),
                    entryNode.get("noop").asBoolean());
            entries.add(logEntry);
        }
        var req = new AppendEntriesRequest(
                body.get("term").asLong(),
                body.get("prev_log_index").asLong(),
                body.get("prev_log_term").asLong(),
                entries,
                body.get("leader_commit").asLong());
        RpcEnvelope<AppendEntriesRequest> reqEnv = envelopeFromBody(body, req);
        var respEnv = raftNode.handleAppendEntries(reqEnv);
        var resp = respEnv.payload();

        ObjectNode reply = mapper.createObjectNode();
        reply.put("type", "append_entries_res");
        putEnvelope(reply, respEnv);
        reply.put("term", resp.term());
        reply.put("success", resp.success());
        transport.reply(src, body.get("msg_id").asLong(), reply);
    }

    // --- helpers -----------------------------------------------------------------------------

    // Envelope metadata (sender/recipient/sentAt/requestId/correlationId) is read straight off
    // the wire body - it must be the sending peer's own values (see MaelstromRaftRpcClient),
    // never fabricated here, or correlationId-based staleness checks in RaftNodeReplicator
    // silently break.
    private static <T> RpcEnvelope<T> envelopeFromBody(JsonNode body, T payload) {
        return new RpcEnvelope<>(
                body.get("sender").asText(),
                body.get("recipient").asText(),
                body.get("sent_at").asLong(),
                body.get("request_id").asLong(),
                body.get("correlation_id").asLong(),
                payload);
    }

    private static void putEnvelope(ObjectNode body, RpcEnvelope<?> envelope) {
        body.put("sender", envelope.sender());
        body.put("recipient", envelope.recipient());
        body.put("sent_at", envelope.sentAt());
        body.put("request_id", envelope.requestId());
        body.put("correlation_id", envelope.correlationId());
    }

    private void replyError(String src, long msgId, int code, String text) {
        ObjectNode reply = mapper.createObjectNode();
        reply.put("type", "error");
        reply.put("code", code);
        reply.put("text", text);
        transport.reply(src, msgId, reply);
    }

    private void replyInternalError(String src, long msgId, Throwable ex) {
        // ex.getMessage() is frequently null (e.g. NullPointerException without an explicit
        // message) - log the full exception with stack trace locally, and include at least the
        // exception's class in the client-facing text so a bare "internal error: null" can never
        // happen again.
        log.error("Internal error handling request from {} (msgId={})", src, msgId, ex);
        replyError(src, msgId, 13, "internal error: " + ex);
    }

    private void replyRaftError(String src, long msgId, RaftError error) {
        if (error instanceof NotLeaderError nle) {
            String hint = nle.getSuggestedLeader() != null ? " leader is " + nle.getSuggestedLeader() : "";
            replyError(src, msgId, 11, "not leader." + hint);
        } else if (error instanceof TimeoutError) {
            replyError(src, msgId, 0, "timeout");
        } else {
            replyError(src, msgId, 13, error.getMessage());
        }
    }

    // Canonical byte[] <-> JsonNode round trip for keys/values, which are arbitrary JSON
    // per Maelstrom's kv protocol - preserving the raw JSON text avoids collapsing e.g. the
    // number 5 and the string "5" into the same bytes.
    private static byte[] jsonToBytes(JsonNode node) {
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }

    private JsonNode bytesToJson(byte[] bytes) {
        try {
            return mapper.readTree(bytes);
        } catch (IOException e) {
            throw new IllegalStateException("stored value is not valid JSON", e);
        }
    }

    public void run() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            transport.handleLine(line);
        }
    }
}
