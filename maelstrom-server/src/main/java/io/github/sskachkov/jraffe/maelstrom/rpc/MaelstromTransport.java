package io.github.sskachkov.jraffe.maelstrom.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public interface MaelstromTransport {
    // One-way message, no reply expected. Prefer reply(...) below for responding to an
    // incoming message - this exists for the (currently hypothetical) case of a genuinely
    // unsolicited outgoing message.
    void send(String dest, ObjectNode body);

    // Replies to an incoming message: sets in_reply_to to inReplyTo and sends to dest.
    // Use this instead of send(...) whenever body is a response to something that arrived.
    void reply(String dest, long inReplyTo, ObjectNode body);

    // A request expecting a correlated reply, resolved by a matching in_reply_to.
    // Fails with a TimeoutException (wrapped) if no reply arrives within timeoutMillis.
    CompletableFuture<JsonNode> request(String dest, ObjectNode body, long timeoutMillis);

    // Registers a handler for incoming messages of the given type that are NOT replies to something we sent.
    void onMessage(String type, BiConsumer<String, JsonNode> handler);
}
