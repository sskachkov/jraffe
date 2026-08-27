package io.github.sskachkov.jraffe.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class App {
    static void main() throws Exception {
        String nodeId = System.getenv("NODE_ID");
        int port = Integer.parseInt(System.getenv("PORT"));
        String clientPortEnv = System.getenv("CLIENT_PORT");
        int clientPort = clientPortEnv != null ? Integer.parseInt(clientPortEnv) : port + 1000;
        Map<String, HostPort> peers = parsePeers(System.getenv("PEERS"));
        peers.remove(nodeId);

        RaftServer server = new RaftServer(nodeId, new HostPort("localhost", port), clientPort, peers);
        server.start();

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            shutdownLatch.countDown();
        }));

        shutdownLatch.await();
    }

    private static Map<String, HostPort> parsePeers(String peers) {
        Map<String, HostPort> result = new HashMap<>();
        for (String entry : peers.split(",")) {
            String[] parts = entry.split(":");
            result.put(parts[0], new HostPort(parts[1], Integer.parseInt(parts[2])));
        }
        return result;
    }
}
