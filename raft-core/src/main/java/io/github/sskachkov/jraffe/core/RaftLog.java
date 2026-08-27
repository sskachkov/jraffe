package io.github.sskachkov.jraffe.core;

import io.github.sskachkov.jraffe.core.rpc.LogEntry;

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
    private List<LogEntry> list;

    public RaftLog() {
        this.list = new ArrayList<>();
        this.list.add(new LogEntry(0, 0, null, true));//dummy entry
    }

    public synchronized long lastIndex() {
        return this.list.size() - 1;
    }

    public synchronized long lastTerm() {
        return this.list.get((int) this.lastIndex()).term();
    }

    public synchronized long termAt(long index) {
        return this.list.get((int) index).term();
    }

    public synchronized LogEntry entryAt(long index) {
        return this.list.get((int) index);
    }

    public synchronized long appendNew(long term, byte[] command, boolean noop) {
        long lastIndex = this.lastIndex();
        LogEntry logEntry = new LogEntry(lastIndex + 1, term, command, noop);
        this.list.add(logEntry);
        return lastIndex + 1;
    }

    public synchronized List<LogEntry> entriesFrom(long fromIndex) {
        return new LinkedList<>(this.list.subList((int) fromIndex, this.list.size()));
    }

    public synchronized Optional<LogEntry> match(long index, long term) {
        if (index >= 0 && this.lastIndex() >= index && this.entryAt(index).term() == term) {
            return Optional.of(this.entryAt(index));
        }
        return Optional.empty();
    }

    public synchronized boolean tryAppend(long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {
        Optional<LogEntry> match = match(prevLogIndex, prevLogTerm);
        if (match.isEmpty()) {
            return false;
        }
        if (newEntries.isEmpty()) {
            return true;
        }

        for (int i = 0; i < newEntries.size(); i++) {
            LogEntry e = newEntries.get(i);
            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it (§5.3)
            if (e.index() <= this.lastIndex()) {
                if (e.term() == this.termAt(e.index())) {
                    continue;
                }
                this.list.subList((int) e.index(), (int) this.lastIndex() + 1).clear();
                this.list.addAll(newEntries.subList(i, newEntries.size()));
                break;
            // Append any new entries not already in the log
            } else {
                this.list.addAll(newEntries.subList(i, newEntries.size()));
                break;
            }
        }
        return true;
    }

    // Election restriction (§5.4.1): is candidate's log at least as up-to-date as ours?
    public synchronized boolean isCandidateUpToDate(long candidateLastIndex, long candidateLastTerm) {
        return candidateLastTerm > this.lastTerm() || (candidateLastTerm == this.lastTerm() && candidateLastIndex >= this.lastIndex());
    }
}