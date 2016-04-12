package cn.v5.lbrpc.thrift.server;

import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.IServer;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-6-18.
 */
public class ThriftRpcServerFactory extends AbstractServerFactory {
    private final int DEFAULT_PORT = 60061;

    @Override
    public IServer createServer() {
        return new ThriftRpcServer(new InetSocketAddress(address, port), registration);
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }
}