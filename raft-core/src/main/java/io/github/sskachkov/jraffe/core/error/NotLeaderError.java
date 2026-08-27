package io.github.sskachkov.jraffe.core.error;

/** This node isn't the leader; carries a suggested leader id when one is known, for forwarding/hinting. */
public class NotLeaderError extends RaftError {
    private final String suggestedLeader;

    public NotLeaderError(String suggestedLeader) {
        this.suggestedLeader = suggestedLeader;
    }

    public String getSuggestedLeader() {
        return suggestedLeader;
    }

    @Override
    public String getMessage() {
        return "Node is not a leader.";
    }
}
