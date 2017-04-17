package cn.v5.lbrpc.common.server;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import cn.v5.lbrpc.http.server.ContainerServerFactory;
import cn.v5.lbrpc.protobuf.server.ProtobufRpcServerFactory;
import cn.v5.lbrpc.thrift.server.ThriftRpcServerFactory;

import java.net.InetAddress;
import java.util.List;

/**
 * Created by yangwei on 15-6-5.
 */
// Factory Method
public abstract class AbstractServerFactory {
    public static final String TOMCAT_CONTAINER = "tomcat";
    public static final String PROTOBUF_RPC = "protobuf";
    public static final String THRIFT_RPC = "thrift";

    protected ServiceRegistration registration;
    protected InetAddress address;
    protected int port = -1;

    public static AbstractServerFactory newFactory(String factoryName) {
        if (factoryName.compareTo(TOMCAT_CONTAINER) == 0) {
            return new ContainerServerFactory(factoryName);
        } else if (factoryName.compareTo(PROTOBUF_RPC) == 0) {
            return new ProtobufRpcServerFactory();
        } else if (factoryName.compareTo(THRIFT_RPC) == 0) {
            return new ThriftRpcServerFactory();
        } else {
            throw new IllegalArgumentException("factory " + factoryName + " invalid");
        }
    }

    public AbstractServerFactory withRegistration(ServiceRegistration registration) {
        this.registration = registration;
        return this;
    }

    public AbstractServerFactory withAddress(InetAddress address) {
        this.address = address;
        return this;
    }

    public AbstractServerFactory withPort(int port) {
        this.port = port;
        return this;
    }

    public abstract LifeCycleServer createServer(List<Object> interceptors);

    public abstract int getDefaultPort();
}