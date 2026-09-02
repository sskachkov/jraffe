package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.rpc.LogEntry;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * The in-memory replicated log: LogEntry list indexed from 1 (index 0 is a fixed dummy
 * sentinel). Handles append, conflict-resolution truncation (&sect;5.3), and the election-safety
 * up-to-date check (&sect;5.4.1). Not persisted &mdash; lost on process restart.
 */
public class RaftLog {
    private final Logger log;

    private final List<LogEntry> list;

    public RaftLog(String nodeId) {
        this.list = new ArrayList<>();
        this.list.add(new LogEntry(0, 0, null, true)); // dummy entry
        this.log = new ContextAwareLogger(RaftLog.class, nodeId);

    }

    public long appendNew(long term, byte[] command, boolean noop) {
        long lastIndex = this.lastIndex();
        LogEntry logEntry = new LogEntry(lastIndex + 1, term, command, noop);
        this.list.add(logEntry);
        return lastIndex + 1;
    }

    long lastIndex() {
        return this.list.size() - 1;
    }

    long lastTerm() {
        LogEntry entry = this.list.get((int) this.lastIndex());
        return entry.term();
    }

    public boolean candidateUpToDate(long candIndex, long candTerm) {
        long lastTerm = this.lastTerm();
        return candTerm > lastTerm || (candTerm == lastTerm && candIndex >= this.lastIndex());
    }

    /**
     * Tries to append newEntries received from the leader. Uses prevLogIndex and prevLogTerm to
     * ensure that leader's log entries right before newEntries are in sync with current log's entries
     * @param prevLogIndex prev log index
     * @param prevLogTerm prev log term
     * @param newEntries entries to append
     * @return append successful
     */
    boolean tryAppend(long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {
        log.debug("tryAppend prevLogIndex: {}, prevLogTerm: {}, newEntries: {}", prevLogIndex, prevLogTerm, newEntries);
        Optional<LogEntry> match = match(prevLogIndex, prevLogTerm);
        if (match.isEmpty()) {
            return false;
        }
        if (newEntries.isEmpty()) {
            return true;
        }

        for (int i = 0; i < newEntries.size(); i++) {
            LogEntry entry = newEntries.get(i);
            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it (§5.3)
            if (entry.index() <= this.lastIndex()) {
                long logsEntryTerm = this.termAt(entry.index());
                if (entry.term() == logsEntryTerm) {
                    continue;
                }
                log.info("Log conflict at index={}: local term={} != leader term={}, truncating {} entries",
                        entry.index(), logsEntryTerm, entry.term(), this.lastIndex() - entry.index() + 1);
                this.list.subList((int) entry.index(), (int) this.lastIndex() + 1).clear();
                this.list.addAll(newEntries.subList(i, newEntries.size()));
                break;
            } else {
                // Append any new entries not already in the log
                this.list.addAll(newEntries.subList(i, newEntries.size()));
                break;
            }
        }
        return true;
    }

    private Optional<LogEntry> match(long index, long term) {
        if (index >= 0 && this.lastIndex() >= index && this.entryAt(index).term() == term) {
            return Optional.of(this.entryAt(index));
        }
        return Optional.empty();
    }

    LogEntry entryAt(long index) {
        return this.list.get((int) index);
    }

    public long termAt(long index) {
        LogEntry entry = this.entryAt(index);
        return entry.term();
    }

    public List<LogEntry> entriesFrom(long index) {
        return new LinkedList<>(this.list.subList((int) index, this.list.size()));
    }
}