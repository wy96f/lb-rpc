package cn.v5.lbrpc.thrift.server;

import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.LifeCycleServer;
import cn.v5.lbrpc.common.server.ServerInterceptor;
import cn.v5.lbrpc.common.utils.CBUtil;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by yangwei on 15-6-18.
 */
public class ThriftRpcServerFactory extends AbstractServerFactory {
    private final int DEFAULT_PORT = 60061;

    @Override
    public LifeCycleServer createServer(List<Object> interceptors) {
        return new ThriftRpcServer(new InetSocketAddress(address, port),
                registration, CBUtil.<ServerInterceptor>convertToClientInterceptor(interceptors));
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }
}