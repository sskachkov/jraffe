package io.github.sskachkov.jraffe.core.rpc;

/**
 * One replicated log entry: index, the term it was created in (fixed forever, even if
 * committed later under a different leader), and either a real command or — if noop — an
 * empty marker a new leader appends to safely establish what's already committed.
 */
public record LogEntry(long index, long term, byte[] command, boolean noop) {
}
