package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import com.google.common.base.Preconditions;

/**
 * Created by yangwei on 15-5-3.
 */
public abstract class AbstractNodeClientBuilder<T extends IRequest, V extends IResponse, B extends AbstractNodeClientBuilder, C extends AbstractNodeClient> {
    ServiceDiscovery serviceDiscoveryImpl;

    Configuration configuration;

    IPipelineAndHeartbeat<T, V> pipelineAndHeartbeat;

    public AbstractNodeClientBuilder() {
    }

    public B withConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return returnBuilder();
    }

    public B withServiceDiscovery(ServiceDiscovery serviceDiscovery) {
        this.serviceDiscoveryImpl = serviceDiscovery;
        return returnBuilder();
    }


    public B withPipelineAndHearbeat(IPipelineAndHeartbeat<T, V> pipelineAndHeartbeat) {
        this.pipelineAndHeartbeat = pipelineAndHeartbeat;
        return returnBuilder();
    }

    protected Configuration getConfiguration() {
        return configuration;
    }

    public AbstractNodeClient<T, V> build(String proto) {
        Preconditions.checkNotNull(serviceDiscoveryImpl, "serverList impl must not be null");

        return createClient(proto);
    }

    protected abstract C createClient(String proto);

    @SuppressWarnings("unchecked")
    protected B returnBuilder() {
        return (B) this;
    }
}
