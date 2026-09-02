package io.github.sskachkov.jraffe.core.node.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RaftNodeElectionScheduler {
    private final RaftNodeImpl raftNode;
    private final ScheduledExecutorService scheduledExecutor;
    private volatile ScheduledFuture<?> electionSchedule;

    public RaftNodeElectionScheduler(RaftNodeImpl raftNode, ScheduledExecutorService scheduledExecutor) {
        this.raftNode = raftNode;
        this.scheduledExecutor = scheduledExecutor;
    }

    /**
     * Schedules a new election. Cancels any existing schedules, and creates a new schedule.
     * Delay is randomized so peers are less likely to start their elections at the same time.
     */
    public void resetElectionTimer() {
        if (this.electionSchedule != null) {
            this.electionSchedule.cancel(false);
        }
        if (!this.raftNode.isRunning()) {
            return;
        }

        int delay = ThreadLocalRandom.current().nextInt(500, 700);
        this.electionSchedule = this.scheduledExecutor.schedule(this.raftNode::beginElection, delay, TimeUnit.MILLISECONDS);
    }

    public void start() {
        this.resetElectionTimer();
    }

    public void stop() {
        if (this.electionSchedule != null) {
            this.electionSchedule.cancel(false);
        }
    }

}
