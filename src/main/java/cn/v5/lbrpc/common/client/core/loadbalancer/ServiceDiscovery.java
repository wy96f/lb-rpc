package cn.v5.lbrpc.common.client.core.loadbalancer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by yangwei on 15-5-3.
 */
public interface ServiceDiscovery {
    public List<InetSocketAddress> getServerList(String service, String proto) throws IOException;
}