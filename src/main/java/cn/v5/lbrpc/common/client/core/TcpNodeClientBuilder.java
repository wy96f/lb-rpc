package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;

/**
 * Created by yangwei on 15-6-24.
 */
public class TcpNodeClientBuilder<T extends IRequest, V extends IResponse> extends AbstractNodeClientBuilder<T, V, TcpNodeClientBuilder<T, V>, AbstractNodeClient<T, V>> {
    @Override
    protected AbstractNodeClient<T, V> createClient(String proto) {
        NettyNodeClient nettyNodeClient = new NettyNodeClient<T, V>(proto, pipelineAndHeartbeat, getConfiguration(), serviceDiscoveryImpl);

        return nettyNodeClient;
    }
}
