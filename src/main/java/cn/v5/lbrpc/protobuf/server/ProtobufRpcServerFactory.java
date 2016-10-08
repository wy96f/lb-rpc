package cn.v5.lbrpc.protobuf.server;

import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.IServer;
import cn.v5.lbrpc.common.server.ServerInterceptor;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by yangwei on 15-6-15.
 */
public class ProtobufRpcServerFactory extends AbstractServerFactory {
    private final int DEFAULT_PORT = 50051;

    @Override
    public IServer createServer(List<Object> interceptors) {
        return new ProtobufRpcServer(new InetSocketAddress(address, port), registration);
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }
}