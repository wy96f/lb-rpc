package cn.v5.lbrpc.protobuf.server;

import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.IServer;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-6-15.
 */
public class ProtobufRpcServerFactory extends AbstractServerFactory {
    private final int DEFAULT_PORT = 50051;

    @Override
    public IServer createServer() {
        return new ProtobufRpcServer(new InetSocketAddress(address, port), registration);
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }
}