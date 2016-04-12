package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.ExceptionCatchingRunnable;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by yangwei on 15-6-9.
 */
public abstract class AbstractNodeClient<T extends IRequest, V extends IResponse> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractNodeClient.class);

    protected final Configuration configuration;
    protected final Map<String, List<InetSocketAddress>> services = Maps.newHashMap();
    protected final String proto;
    private final ServiceDiscovery serviceDiscoveryImpl;
    protected AbstractManager<T, V> manager;
    protected ScheduledExecutorService scheduleExecutor = new ScheduledThreadPoolExecutor(1);
    protected Runnable refreshTask = new RefreshServerList();

    public AbstractNodeClient(String proto, Configuration configuration, ServiceDiscovery serviceDiscoveryImpl) {
        this.proto = proto;
        this.configuration = configuration;

        this.serviceDiscoveryImpl = serviceDiscoveryImpl;

        this.refreshTask = new RefreshServerList();

        scheduleExecutor.scheduleWithFixedDelay(refreshTask, 30, 60, TimeUnit.SECONDS);
    }

    public synchronized List<InetSocketAddress> addService(String service) throws IOException {
        if (services.get(service) != null) {
            return services.get(service);
        }
        List<InetSocketAddress> latestAddresses = serviceDiscoveryImpl.getServerList(service, proto);
        services.put(service, latestAddresses);
        return latestAddresses;
    }

    public AbstractManager<T, V> getManager() {
        return manager;
    }

    public CloseFuture close() {
        scheduleExecutor.shutdownNow();
        try {
            scheduleExecutor.awaitTermination(4, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return manager.close();
    }

    public void serverListChanged(String service, List<InetSocketAddress> oldHosts, List<InetSocketAddress> newHosts) {
        if (newHosts == null || newHosts.size() == 0) {
            logger.warn("new hosts {} empty, old hosts {}", newHosts, oldHosts);
            return;
        }

        List<InetSocketAddress> cloneHosts = Lists.newArrayList(oldHosts);
        for (InetSocketAddress address : newHosts) {
            Host host = manager.getHost(address);
            // we have not found this host ever or seen this since last time
            if (host == null) {
                Host newHost = manager.addHost(Pair.create(service, proto), address);
                if (newHost == null) {
                    logger.error("add address {} raced, this should not happen since we're in single thread pool", address);
                    return;
                }
                logger.debug("oldHosts {} and newHosts {}, triggerOnAdd {}", oldHosts, newHosts, address);
                manager.triggerOnAdd(newHost);
            } else if (!oldHosts.contains(address)) {
                logger.debug("oldHosts {} and newHosts {}, triggerOnUp {}", oldHosts, newHosts, address);
                host.addService(Pair.create(service, proto));
                manager.triggerOnUp(host);
            } else if (!host.isUp()) {
                logger.debug("host {} is not up, try to on up it", address);
                host.addService(Pair.create(service, proto));
                manager.triggerOnUp(host);
                cloneHosts.remove(host.getAddress());
            } else {
                cloneHosts.remove(host.getAddress());
            }
        }

        /**
         * we don't on down the old hosts which are not in the list of new discovered hosts
         * since the hosts maybe up, particularly in the case of split brain
         */
        for (InetSocketAddress address : cloneHosts) {
            logger.debug("host {} down?", address);
            Host host = manager.getHost(address);
            Pair<String, String> serviceAndProto = Pair.create(service, proto);
            manager.triggerOnServiceRemoval(host, serviceAndProto);
        }
        services.put(service, newHosts);
    }

    // for the moment it's not thread safe, so only one thread scheduler
    protected class RefreshServerList extends ExceptionCatchingRunnable {
        @Override
        public void runMayThrow() throws Exception {
            for (String service : services.keySet()) {
                serverListChanged(service, services.get(service), serviceDiscoveryImpl.getServerList(service, proto));
            }
        }
    }

    @VisibleForTesting
    public void forceRefresh() throws ExecutionException, InterruptedException {
        Future<?> refresh = scheduleExecutor.submit(refreshTask);
        refresh.get();
    }
}
