package io.github.sskachkov.jraffe.core.error;

/** Base type for a structured reason a client submission failed. */
public abstract class RaftError {
    public abstract String getMessage();
}
