package io.github.sskachkov.jraffe.core.rpc;

/** Thrown by a RaftRpcClient implementation when a peer RPC couldn't complete. */
public class RaftRpcException extends Exception {
    public RaftRpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
