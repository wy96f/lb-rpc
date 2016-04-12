package cn.v5.lbrpc;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangwei on 22/7/15.
 */
public class InMemoryServiceDiscoveryAndRegistration implements ServiceDiscovery, ServiceRegistration {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryServiceDiscoveryAndRegistration.class);

    private final Multimap<Pair<String, String>, InetSocketAddress> services = ArrayListMultimap.create();

    @Override
    public List<InetSocketAddress> getServerList(String service, String proto) throws IOException {
        return new ArrayList<>(services.get(Pair.create(service, proto)));
    }

    @Override
    public synchronized void registerServer(String serviceName, String proto, InetSocketAddress address) {
        logger.info("register service {} with proto {} and address {}", serviceName, proto, address);
        Pair<String, String> service = Pair.create(serviceName, proto);
        if (services.get(service) != null && services.get(service).contains(address)) {
            logger.warn("registering same service at the same port");
            return;
        }
        services.put(service, address);
    }

    @Override
    public synchronized ListenableFuture<?> unregisterServer(String serviceName, String proto, InetSocketAddress address) {
        logger.info("unregister service {} with proto {} and address {}", serviceName, proto, address);
        Pair<String, String> service = Pair.create(serviceName, proto);
        services.remove(service, address);
        return null;
    }
}
