package cn.v5.lbrpc.http.server;

import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.LifeCycleServer;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by yangwei on 15-6-15.
 */
public class ContainerServerFactory extends AbstractServerFactory {
    private final int DEFAULT_PORT = 60051;

    private final String proto;

    public ContainerServerFactory(String proto) {
        this.proto = proto;
    }

    @Override
    public LifeCycleServer createServer(List<Object> interceptors) {
        ContainerServer server = new ContainerServer(new InetSocketAddress(address, port), interceptors, registration, proto);
        return server;
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }
}