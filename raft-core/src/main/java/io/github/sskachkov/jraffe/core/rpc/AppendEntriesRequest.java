package io.github.sskachkov.jraffe.core.rpc;

import java.util.List;

public record AppendEntriesRequest(long term, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) implements RpcPayload {}
