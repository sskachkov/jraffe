package io.github.sskachkov.jraffe.core.message;

import io.github.sskachkov.jraffe.core.error.NotLeaderError;
import io.github.sskachkov.jraffe.core.error.RaftError;
import io.github.sskachkov.jraffe.core.error.TimeoutError;

/**
 * The outcome of a client request: success with result bytes, or failure with a RaftError
 * that distinguishes a definite failure from an indefinite/timeout one.
 */
public class RaftResponse {
    private final long requestId;
    private final String nodeId;
    private final boolean success;
    private final byte[] data;
    private final RaftError error;

    private RaftResponse(long requestId, String nodeId, boolean success, byte[] data, RaftError error) {
        this.requestId = requestId;
        this.nodeId = nodeId;
        this.success = success;
        this.data = data;
        this.error = error;
    }

    public static RaftResponse success(long requestId, String nodeId, byte[] data) {
        return new RaftResponse(requestId, nodeId, true, data, null);
    }

    public static RaftResponse failure(long requestId, String nodeId, RaftError error) {
        return new RaftResponse(requestId, nodeId, false, null, error);
    }

    public static RaftResponse timeoutError(long requestId, String nodeId) {
        return new RaftResponse(requestId, nodeId, false, null, new TimeoutError());
    }

    public static RaftResponse notLeaderError(long requestId, String nodeId, String leaderId) {
        return new RaftResponse(requestId, nodeId, false, null, new NotLeaderError(leaderId));
    }

    public long getRequestId() {
        return requestId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isSuccess() {
        return success;
    }

    public byte[] getData() {
        return data;
    }

    public RaftError getError() {
        return error;
    }
}
