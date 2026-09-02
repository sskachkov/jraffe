package io.github.sskachkov.jraffe.core.error;

public class RaftInternalError  extends RaftError {
    private final String message;

    public RaftInternalError(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
