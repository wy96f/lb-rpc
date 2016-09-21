package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.*;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.client.core.loadbalancer.RoundRobinPolicy;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.client.core.policies.BoundedExponentialReconnectionPolicy;
import cn.v5.lbrpc.common.client.core.policies.ReconnectionPolicy;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.protobuf.client.core.ProtobufPipelineConfigurator;
import cn.v5.lbrpc.thrift.client.core.ThriftPipelineConfigurator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yangwei on 15-6-15.
 */
public class RpcProxyFactory {
    private static final Logger logger = LoggerFactory.getLogger(RpcProxyFactory.class);

    public static Map<String, AbstractNodeClient<?, ?>> nodeClients = Maps.newHashMap();

    public static List<String> supports = ImmutableList.of(CBUtil.HTTP_PROTO, CBUtil.PROTOBUF_PROTO, CBUtil.THRIFT_PROTO);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("Run shutdown hook of rpc proxy factory now");
                close();
                logger.info("Rpc proxy factory shutdown completed");
            }
        }));
    }

    public static RpcProxyFactoryBuilder createBuilder() {
        return new RpcProxyFactoryBuilder();
    }

    private static AbstractNodeClient<?, ?> createNodeClient(String proto, ServiceDiscovery sd, Configuration configuration) {
        if (proto.compareTo(CBUtil.HTTP_PROTO) == 0) {
            return NodeFactory.<IRequest, IResponse>createHttpNodeClient(sd, configuration);
        } else if (proto.compareTo(CBUtil.PROTOBUF_PROTO) == 0) {
            return NodeFactory.createTcpNodeClient(CBUtil.PROTOBUF_PROTO, new ProtobufPipelineConfigurator(), sd, configuration);
        } else if (proto.compareTo(CBUtil.THRIFT_PROTO) == 0) {
            return NodeFactory.createTcpNodeClient(CBUtil.THRIFT_PROTO, new ThriftPipelineConfigurator(), sd, configuration);
        } else {
            throw new IllegalArgumentException("proto " + proto + " not supported");
        }
    }

    public static void close() {
        List<CloseFuture> closeFutures = Lists.newArrayList();
        for (Map.Entry<String, AbstractNodeClient<?, ?>> nodeClientEntry : nodeClients.entrySet()) {
            logger.info("close node client of proto {}", nodeClientEntry.getKey());
            closeFutures.add(nodeClientEntry.getValue().close().force());
        }
        ListenableFuture<List<Void>> finalCloseFuture = Futures.allAsList(closeFutures);
        try {
            finalCloseFuture.get(8, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (TimeoutException e) {
            logger.error("waiting for closing rpc proxy factory timeout");
        } catch (ExecutionException e) {
            logger.error("Unexpected error while closing rpc proxy factory", e.getCause());
        }
        nodeClients.clear();
    }

    public static class RpcProxyFactoryBuilder {
        public ServiceDiscovery sd;
        public LoadBalancingPolicy loadBalancingPolicy;
        public ReconnectionPolicy reconnectionPolicy;
        public PoolOptions poolOptions;
        public SocketOptions socketOptions;

        public RpcProxyFactoryBuilder withServiceDiscovery(ServiceDiscovery sd) {
            this.sd = sd;
            return this;
        }

        public RpcProxyFactoryBuilder withLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
            this.loadBalancingPolicy = loadBalancingPolicy;
            return this;
        }

        public RpcProxyFactoryBuilder withReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
            this.reconnectionPolicy = reconnectionPolicy;
            return this;
        }

        public RpcProxyFactoryBuilder withPoolOptions(PoolOptions poolOptions) {
            this.poolOptions = poolOptions;
            return this;
        }

        public RpcProxyFactoryBuilder withSocketOptions(SocketOptions socketOptions) {
            this.socketOptions = socketOptions;
            return this;
        }

        public <T> T create(Class<T> clazz, String proto) throws Exception {
            return create(clazz, proto, false);
        }

        public <T> T createAsync(Class<T> clazz, String proto) throws Exception {
            return create(clazz, proto, true);
        }

        public <T> T create(Class<T> clazz, String proto, boolean async) throws Exception {
            Preconditions.checkArgument(supports.contains(proto), "proto " + proto + " not supported");
            Preconditions.checkNotNull(sd, "service discovery must not be null");

            Configuration configuration = getConfiguration();

            synchronized (nodeClients) {
                AbstractNodeClient nodeClient = nodeClients.get(proto);
                if (nodeClient == null) {
                    nodeClient = createNodeClient(proto, sd, configuration);
                    nodeClients.put(proto, nodeClient);
                }

                if (proto.compareTo(CBUtil.HTTP_PROTO) == 0) {
                    if (async) throw new UnsupportedOperationException("Http not support async proxy");
                    return new ProxyBuilder<T>(clazz, nodeClient).buildHttp(getLoadBalancingPolicy());
                } else if (proto.compareTo(CBUtil.PROTOBUF_PROTO) == 0) {
                    if (async) return new ProxyBuilder<T>(clazz, nodeClient).buildProtobufAsync(getLoadBalancingPolicy());
                    else return new ProxyBuilder<T>(clazz, nodeClient).buildProtobuf(getLoadBalancingPolicy());
                } else {
                    if (async) return new ProxyBuilder<T>(clazz, nodeClient).buildThriftAsync(getLoadBalancingPolicy());
                    else return new ProxyBuilder<T>(clazz, nodeClient).buildThrift(getLoadBalancingPolicy());
                }
            }
        }

        protected LoadBalancingPolicy getLoadBalancingPolicy() {
            return loadBalancingPolicy == null ? new RoundRobinPolicy() : loadBalancingPolicy;
        }

        protected Configuration getConfiguration() {
            return new Configuration(reconnectionPolicy == null ? new BoundedExponentialReconnectionPolicy(2000, 300000, 10) : reconnectionPolicy,
                    poolOptions == null ? new PoolOptions() : poolOptions,
                    socketOptions == null ? new SocketOptions() : socketOptions);
        }
    }
}