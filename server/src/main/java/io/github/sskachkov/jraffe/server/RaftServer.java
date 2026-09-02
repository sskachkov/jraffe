package io.github.sskachkov.jraffe.server;

import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.server.metrics.MetricsFormatter;
import io.github.sskachkov.jraffe.server.protocol.ClientProtocolServer;
import io.github.sskachkov.jraffe.server.rpc.RaftGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;

public class RaftServer {
    private final Logger log;

    private final Server server;
    private final RaftNode node;
    private final MeterRegistry registry;
    private final ClientProtocolServer clientProtocolServer;

    public RaftServer(RaftNode node, HostPort addr, int clientPort, Map<String, HostPort> peers, MeterRegistry registry) throws IOException {
        this.log = new ContextAwareLogger(RaftServer.class, node.getId());
        this.registry = registry;
        log.info("Creating node, addr={}, clientPort={}, peers={}.", addr, clientPort, peers);
        this.node = node;
        this.server = ServerBuilder.forPort(addr.port())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new RaftGrpcService(this.node, node.getId()))
                .build();
        this.clientProtocolServer = new ClientProtocolServer(this.node, this.registry, node.getId(), clientPort);
    }

    public void start() throws IOException {
        log.info("Starting..");
        server.start();
        clientProtocolServer.start();
        node.start();
    }

    public void stop() {
        log.info("Stopping..");
        log.info("Stats:\n{}", String.join("\n", MetricsFormatter.format(registry)));
        try {
            clientProtocolServer.shutdown();
        } catch (IOException e) {
            log.error("Error shutting down client protocol server", e);
        }
        server.shutdown();
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        node.stop();
    }

}
