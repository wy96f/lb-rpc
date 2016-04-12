package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by yangwei on 15-5-5.
 */
public class NettyNodeClient<T extends IRequest, V extends IResponse> extends AbstractNodeClient<T, V> {
    public NettyNodeClient(String proto, IPipelineAndHeartbeat pipelineAndHearbeat, Configuration configuration, ServiceDiscovery serviceDiscoveryImpl) {
        super(proto, configuration, serviceDiscoveryImpl);
        this.manager = new NettyManager<T, V>(pipelineAndHearbeat, configuration);
    }
}