package io.github.sskachkov.jraffe.core.error;

/** Indefinite outcome — the submission confirmed neither success nor definite failure within the request timeout. */
public class TimeoutError extends RaftError {

    @Override
    public String getMessage() {
        return "Timeout Error";
    }
}
