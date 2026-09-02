package io.github.sskachkov.jraffe.core.node.impl;

import io.github.sskachkov.jraffe.core.node.RaftNode;
import io.github.sskachkov.jraffe.core.rpc.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

// routes RaftRpcClient calls directly between in-process RaftNode instances, no network involved.
// a node marked "down" is unreachable in both directions, simulating a crashed process.
public class InMemoryCluster {
    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, TestStateMachineAdapter> stateMachines = new ConcurrentHashMap<>();
    private final Set<String> down = ConcurrentHashMap.newKeySet();

    public List<RaftNode> start(List<String> nodeIds) {
        List<RaftNode> nodes = new ArrayList<>();
        for (String id : nodeIds) {
            List<String> peers = nodeIds.stream().filter(other -> !other.equals(id)).toList();
            TestStateMachineAdapter stateMachine = new TestStateMachineAdapter();
            this.stateMachines.put(id, stateMachine);
            RaftNode node = new RaftNodeImpl(id, peers, this.clientFor(id), stateMachine, new SimpleMeterRegistry());
            this.add(node);
            nodes.add(node);
        }
        nodes.forEach(RaftNode::start);
        return nodes;
    }

    TestStateMachineAdapter stateMachineFor(String nodeId) {
        return this.stateMachines.get(nodeId);
    }

    public void add(RaftNode node) {
        nodes.put(node.getId(), node);
    }

    // marks a node unreachable in both directions while leaving it fully running -- simulates a
    // network partition, not a process crash. Reversible via heal().
    public void isolate(String nodeId) {
        down.add(nodeId);
    }

    public void heal(String nodeId) {
        down.remove(nodeId);
    }

    // simulates the node's process actually dying: isolated AND its own executors stopped.
    // one-way, same as a real crash -- shutdown() cannot be un-done.
    public void crash(String nodeId) {
        isolate(nodeId);
        nodes.get(nodeId).stop();
    }

    // simulates a crash followed by a process restart: the old RaftNode (and its background
    // threads) is discarded and a brand new one takes its place under the same id/peers. There
    // is no persistence layer yet, so the new instance starts with fresh in-memory state --
    // currentTerm/votedFor/log are all gone, exactly like a real restart would be today.
    RaftNode restart(String nodeId) {
        RaftNode old = nodes.get(nodeId);
        List<String> peers = old.getPeerIds();
        old.stop();
        TestStateMachineAdapter stateMachine = new TestStateMachineAdapter();
        this.stateMachines.put(nodeId, stateMachine);
        RaftNodeImpl fresh = new RaftNodeImpl(nodeId, peers, this.clientFor(nodeId), stateMachine, new SimpleMeterRegistry());
        this.add(fresh);
        fresh.start();
        return fresh;
    }

    // shuts down whatever is currently registered under each id -- unlike iterating a
    // test's own List<RaftNode>, this stays correct after restart() replaces an entry.
    public void shutdownAll() {
        nodes.values().forEach(RaftNode::stop);
    }

    public RaftRpcClient clientFor(String selfId) {
        return new RaftRpcClient() {
            @Override
            public RpcEnvelope<RequestVoteResponse> requestVote(RpcEnvelope<RequestVoteRequest> env) throws RaftRpcException {
                return dispatch(env.recipient(), node -> node.handleRequestVote(env));
            }

            @Override
            public RpcEnvelope<AppendEntriesResponse> appendEntries(RpcEnvelope<AppendEntriesRequest> env) throws RaftRpcException {
                return dispatch(env.recipient(), node -> node.handleAppendEntries(env));
            }

            private <T> T dispatch(String peerId, Function<RaftNode, T> call) throws RaftRpcException {
                if (down.contains(selfId) || down.contains(peerId)) {
                    throw new RaftRpcException("peer unreachable: " + peerId, null);
                }
                RaftNode target = nodes.get(peerId);
                if (target == null) {
                    throw new RaftRpcException("unknown peer: " + peerId, null);
                }
                return call.apply(target);
            }
        };
    }
}
