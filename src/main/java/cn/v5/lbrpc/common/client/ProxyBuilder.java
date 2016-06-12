package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.http.client.HttpRpcProxy;
import cn.v5.lbrpc.protobuf.client.ProtobufRpcProxy;
import cn.v5.lbrpc.protobuf.data.ProtobufMessage;
import cn.v5.lbrpc.thrift.client.ThriftRpcProxy;
import cn.v5.lbrpc.thrift.data.ThriftMessage;

/**
 * Created by yangwei on 15-6-15.
 */
// abstract factory
public class ProxyBuilder<T> {
    private final Class<T> iface;
    private final AbstractNodeClient nodeClient;
    private ClassLoader loader;

    public ProxyBuilder(Class<T> iface, AbstractNodeClient nodeClient) {
        this.iface = iface;
        this.nodeClient = nodeClient;
        this.loader = iface.getClassLoader();
    }

    public T buildProtobuf(LoadBalancingPolicy loadBalancingPolicy) throws Exception {
        return new SyncRpcProxy<T, ProtobufMessage, ProtobufMessage>(nodeClient, new ProtobufRpcProxy<T>(nodeClient, iface, loadBalancingPolicy)).proxy();
    }

    public T buildThrift(LoadBalancingPolicy loadBalancingPolicy) throws Exception {
        return new SyncRpcProxy<T, ThriftMessage, ThriftMessage>(nodeClient, new ThriftRpcProxy<T>(nodeClient, iface, loadBalancingPolicy)).proxy();
    }

    public T buildProtobufAsync(LoadBalancingPolicy loadBalancingPolicy) throws Exception {
        return new AsyncRpcProxy<T, ProtobufMessage, ProtobufMessage>(nodeClient, new ProtobufRpcProxy<T>(nodeClient, iface, loadBalancingPolicy)).proxy();
    }

    public T buildThriftAsync(LoadBalancingPolicy loadBalancingPolicy) throws Exception {
        return new AsyncRpcProxy<T, ThriftMessage, ThriftMessage>(nodeClient, new ThriftRpcProxy<T>(nodeClient, iface, loadBalancingPolicy)).proxy();
    }

    // TODO sync or async rpc proxy
    public T buildHttp(LoadBalancingPolicy loadBalancingPolicy) throws Exception {
        return new HttpRpcProxy<T>(nodeClient, iface, loadBalancingPolicy).proxy();
    }
}