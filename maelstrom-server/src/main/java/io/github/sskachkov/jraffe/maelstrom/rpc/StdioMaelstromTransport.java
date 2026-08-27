package io.github.sskachkov.jraffe.maelstrom.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

// Owns the process's stdout (writes) and is fed stdin lines one at a time via handleLine(...)
// by whatever owns the actual read loop (the process entrypoint). Every message that carries
// in_reply_to is treated as a reply and resolves a pending request(); everything else is routed
// to a handler registered via onMessage(...).
//
// nodeId isn't known at construction time - Maelstrom only tells a node its own id via the
// "init" message, which arrives at runtime. init(...) sets it once that happens; nothing that
// sends a message (send/reply/request) may be called before init(...) has run.
public class StdioMaelstromTransport implements MaelstromTransport {
    private final Logger log = LoggerFactory.getLogger(StdioMaelstromTransport.class);
    private volatile String nodeId;
    private final ObjectMapper mapper;
    private final PrintStream out;
    private final AtomicLong msgIdCounter = new AtomicLong();
    private final Map<Long, CompletableFuture<JsonNode>> pending = new ConcurrentHashMap<>();
    private final Map<String, BiConsumer<String, JsonNode>> handlers = new ConcurrentHashMap<>();

    public StdioMaelstromTransport(ObjectMapper mapper, PrintStream out) {
        this.mapper = mapper;
        this.out = out;
    }

    // Called once, by the "init" message handler, once Maelstrom has told us our node id.
    public void init(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void send(String dest, ObjectNode body) {
        body.put("msg_id", msgIdCounter.incrementAndGet());
        write(envelope(dest, body));
    }

    @Override
    public void reply(String dest, long inReplyTo, ObjectNode body) {
        body.put("msg_id", msgIdCounter.incrementAndGet());
        body.put("in_reply_to", inReplyTo);
        write(envelope(dest, body));
    }

    @Override
    public CompletableFuture<JsonNode> request(String dest, ObjectNode body, long timeoutMillis) {
        long msgId = msgIdCounter.incrementAndGet();
        body.put("msg_id", msgId);

        CompletableFuture<JsonNode> future = new CompletableFuture<>();
        future.orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        pending.put(msgId, future);
        future.whenComplete((reply, ex) -> pending.remove(msgId));

        write(envelope(dest, body));
        return future;
    }

    @Override
    public void onMessage(String type, BiConsumer<String, JsonNode> handler) {
        handlers.put(type, handler);
    }

    // Called once per line read from stdin by the process's main loop.
    public void handleLine(String line) {
        JsonNode envelope;
        try {
            envelope = mapper.readTree(line);
        } catch (Exception e) {
            log.warn("failed to parse incoming line, skipping: {}", line, e);
            return;
        }

        String src = envelope.get("src").asText();
        JsonNode body = envelope.get("body");
        JsonNode inReplyTo = body.get("in_reply_to");
        if (inReplyTo != null) {
            CompletableFuture<JsonNode> future = pending.remove(inReplyTo.asLong());
            if (future != null) {
                future.complete(body);
            } else {
                log.trace("no pending request for in_reply_to={}, ignoring", inReplyTo.asLong());
            }
            return;
        }

        String type = body.get("type").asText();
        BiConsumer<String, JsonNode> handler = handlers.get(type);
        if (handler != null) {
            handler.accept(src, body);
        } else {
            log.warn("no handler registered for message type '{}'", type);
        }
    }

    private ObjectNode envelope(String dest, ObjectNode body) {
        if (nodeId == null) {
            throw new IllegalStateException("init(nodeId) must be called before sending any message");
        }
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("src", nodeId);
        envelope.put("dest", dest);
        envelope.set("body", body);
        return envelope;
    }

    private void write(ObjectNode envelope) {
        synchronized (out) {
            out.println(envelope.toString());
            out.flush();
        }
    }
}
