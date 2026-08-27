package io.github.sskachkov.jraffe.core;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

public class RaftTestUtils {
    public static Optional<RaftNode> awaitLeader(List<RaftNode> nodes, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (RaftNode node : nodes) {
                if (node.getRole() == Role.LEADER) {
                    return Optional.of(node);
                }
            }
            Thread.sleep(20);
        }
        return Optional.empty();
    }

    public static boolean awaitCondition(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

}
