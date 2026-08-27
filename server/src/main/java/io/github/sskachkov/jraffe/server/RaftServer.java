package io.github.sskachkov.jraffe.server;

import io.github.sskachkov.jraffe.core.RaftNode;
import io.github.sskachkov.jraffe.core.StateMachine;
import io.github.sskachkov.jraffe.core.logging.ContextAwareLogger;
import io.github.sskachkov.jraffe.kvstore.KVStoreImpl;
import io.github.sskachkov.jraffe.server.metrics.MetricsFormatter;
import io.github.sskachkov.jraffe.server.protocol.ClientProtocolServer;
import io.github.sskachkov.jraffe.server.rpc.GrpcRaftRpcClient;
import io.github.sskachkov.jraffe.server.rpc.RaftGrpcService;
import io.github.sskachkov.jraffe.server.statemachine.KvStoreStateMachine;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;

public class RaftServer {
    private final Logger log;

    private final String nodeId;
    private final Server server;
    private final RaftNode node;
    private final MeterRegistry registry;
    private final ClientProtocolServer clientProtocolServer;

    public RaftServer(String nodeId, HostPort addr, int clientPort, Map<String, HostPort> peers) throws IOException {
        this.log = new ContextAwareLogger(RaftServer.class, nodeId);
        this.nodeId = nodeId;
        this.registry = new SimpleMeterRegistry();
        log.info("Creating node, addr={}, clientPort={}, peers={}.", addr, clientPort, peers);
        GrpcRaftRpcClient client = new GrpcRaftRpcClient(peers, nodeId, this.registry);
        StateMachine stateMachine = new KvStoreStateMachine(new KVStoreImpl());
        this.node = new RaftNode(nodeId, peers.keySet().stream().toList(), client, stateMachine, this.registry);
        this.server = ServerBuilder.forPort(addr.port())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new RaftGrpcService(this.node, nodeId))
                .build();
        this.clientProtocolServer = new ClientProtocolServer(this.node, this.registry, nodeId, clientPort);
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
        node.shutdown();
    }

}
