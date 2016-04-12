package cn.v5.lbrpc.common.client.core.loadbalancer;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yangwei on 15-5-5.
 */
public class FixedServiceDiscovery implements ServiceDiscovery {
    private final List<InetSocketAddress> hosts;

    public FixedServiceDiscovery(InetSocketAddress... hosts) {
        this.hosts = Arrays.asList(hosts);
    }

    @Override
    public List<InetSocketAddress> getServerList(String service, String proto) {
        return hosts;
    }
}
