package io.github.sskachkov.jraffe.core.node;

/**
 * Read-only snapshot of one peer's replication progress, as seen by the current leader.
 * lastConfirmedDispatchAt is a {@link System#nanoTime()} value, not wall-clock time -- only
 * meaningful as a delta against another nanoTime() reading (e.g. "how long ago").
 */
public record PeerReplicationStatus(long matchIndex, long nextIndex, long lastConfirmedDispatchAt) {}
