package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.client.core.loadbalancer.RoundRobinPolicy;
import cn.v5.lbrpc.common.client.core.policies.ReconnectionPolicy;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by yangwei on 15-5-26.
 */
public class Configuration {
    private final ReconnectionPolicy reconnectionPolicy;
    private final Map<Pair<String, String>, LoadBalancingPolicy> loadBalancingPolicy = Maps.newHashMap();

    private final PoolOptions poolOptions;
    private final SocketOptions socketOptions;

    public Configuration(ReconnectionPolicy reconnectionPolicy, PoolOptions poolOptions, SocketOptions socketOptions) {
        this.reconnectionPolicy = reconnectionPolicy;
        this.poolOptions = poolOptions;
        this.socketOptions = socketOptions;
    }

    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    public synchronized void addLoadBalancingPolicy(Pair<String, String> serviceAndProto, LoadBalancingPolicy policy) {
        loadBalancingPolicy.putIfAbsent(serviceAndProto, policy == null ? new RoundRobinPolicy() : policy);
    }

    public LoadBalancingPolicy getLoadBalancingPolicy(Pair<String, String> serviceAndProto) {
        return loadBalancingPolicy.get(serviceAndProto);
    }

    public PoolOptions getPoolOptions() {
        return poolOptions;
    }

    public SocketOptions getSocketOptions() {
        return socketOptions;
    }
}
