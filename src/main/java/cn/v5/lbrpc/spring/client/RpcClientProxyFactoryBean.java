package cn.v5.lbrpc.spring.client;

import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.PoolOptions;
import cn.v5.lbrpc.common.client.core.SocketOptions;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.client.core.policies.ReconnectionPolicy;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Created by yangwei on 7/7/15.
 */
public class RpcClientProxyFactoryBean<T> implements FactoryBean<T>, InitializingBean {
    public ReconnectionPolicy reconnectionPolicy;
    public PoolOptions poolOptions;
    public SocketOptions socketOptions;
    private Class<T> serviceInterface;
    private String proto;
    private ServiceDiscovery sd;
    private LoadBalancingPolicy loadBalancingPolicy;
    private T client;

    @Override
    public T getObject() throws Exception {
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return serviceInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public Class<T> getServiceInterface() {
        return serviceInterface;
    }

    public void setServiceInterface(Class<T> serviceInterface) {
        this.serviceInterface = serviceInterface;
    }

    public String getProto() {
        return proto;
    }

    public void setProto(String proto) {
        this.proto = proto;
    }

    public ServiceDiscovery getSd() {
        return sd;
    }

    public void setSd(ServiceDiscovery sd) {
        this.sd = sd;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        client = RpcProxyFactory.createBuilder()
                .withServiceDiscovery(sd)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withPoolOptions(poolOptions)
                .withReconnectionPolicy(reconnectionPolicy)
                .withSocketOptions(socketOptions)
                .create(serviceInterface, proto);
    }
}
