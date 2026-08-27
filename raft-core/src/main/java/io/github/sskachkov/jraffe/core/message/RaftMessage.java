package io.github.sskachkov.jraffe.core.message;

/**
 * A client request's payload, plus a caller-supplied id used to correlate the eventual RaftResponse.
 * Payload is intentionally opaque.
 */
public record RaftMessage(long id, byte[] data) {
    public RaftMessage(byte[] data) {
        this(-1, data);
    }

}
