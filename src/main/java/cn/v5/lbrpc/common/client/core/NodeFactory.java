package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.CBUtil;

/**
 * Created by yangwei on 15-6-24.
 */
public class NodeFactory {
    public static <T extends IRequest, V extends IResponse> AbstractNodeClient<T, V> createTcpNodeClient(String proto, IPipelineAndHeartbeat<T, V> pipelineAndHeartbeat,
                                                                                                         ServiceDiscovery sd, Configuration configuration) {
        return new TcpNodeClientBuilder<T, V>().withPipelineAndHearbeat(pipelineAndHeartbeat).withServiceDiscovery(sd).withConfiguration(configuration).build(proto);
    }

    public static <T extends IRequest, V extends IResponse> AbstractNodeClient<T, V> createHttpNodeClient(ServiceDiscovery sd, Configuration configuration) {
        return new HttpNodeClientBuilder<T, V>().withServiceDiscovery(sd).withConfiguration(configuration).build(CBUtil.HTTP_PROTO);
    }
}
