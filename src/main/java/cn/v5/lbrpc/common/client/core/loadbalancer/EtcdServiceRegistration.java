package cn.v5.lbrpc.common.client.core.loadbalancer;

import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.ExceptionCatchingRunnable;
import cn.v5.lbrpc.common.utils.InetSocketAddressUtil;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.justinsb.etcd.EtcdClient;
import com.justinsb.etcd.EtcdClientException;
import com.justinsb.etcd.EtcdResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yangwei on 15-6-4.
 */
public class EtcdServiceRegistration implements ServiceRegistration {
    private static final Logger logger = LoggerFactory.getLogger(EtcdServiceRegistration.class);
    private static final String SERVICE_PREFIX = CBUtil.SERVICE_PREFIX;
    public final EtcdClient client;
    private final Multimap<Pair<String, String>, InetSocketAddress> services = ArrayListMultimap.create();

    protected ListeningScheduledExecutorService scheduleExecutor = MoreExecutors.listeningDecorator(new ScheduledThreadPoolExecutor(1));
    protected final ListenableScheduledFuture<?> scheduledRefreshFuture;

    public EtcdServiceRegistration(String url) {
        this.client = new EtcdClient(URI.create(url));
        this.scheduledRefreshFuture = scheduleExecutor.scheduleWithFixedDelay(new RefreshServices(), 20, 20, TimeUnit.SECONDS);
    }

    public void update(String service, String proto, InetSocketAddress address) {
        try {
            String key = InetSocketAddressUtil.serializeAddress(address);
            EtcdResult result = client.set("/" + proto + SERVICE_PREFIX + service + "/" + key, "", 30);
            logger.debug("update service {}", "/" + proto + SERVICE_PREFIX + service + "/" + key);
            if (result.isError()) {
                logger.error("register service " + service + " with address " + address + " occurs error " + result.toString());
            }
        } catch (EtcdClientException e) {
            logger.error("register service " + service + " with address " + address + " occurs exception: ", e);
        }
    }

    public void remove(String service, String proto, InetSocketAddress address) {
        try {
            String key = InetSocketAddressUtil.serializeAddress(address);
            EtcdResult result = client.delete("/" + proto + SERVICE_PREFIX + service + "/" + key);
            logger.debug("unregister service {}", "/" + proto + SERVICE_PREFIX + service + "/" + key);
            if (result.isError()) {
                logger.error("unregister service " + service + " with address " + address + " occurs error " + result.toString());
            }
        } catch (EtcdClientException e) {
            logger.error("unregister service " + service + " with address " + address + " occurs exception: ", e);
        }
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
        update(serviceName, proto, address);
    }

    @Override
    public synchronized ListenableFuture<?> unregisterServer(String serviceName, String proto, InetSocketAddress address) {
        logger.info("unregister service {} with proto {} and address {}", serviceName, proto, address);
        if (!scheduledRefreshFuture.isCancelled() && !scheduledRefreshFuture.isDone()) {
            scheduledRefreshFuture.cancel(true);
        }
        Pair<String, String> service = Pair.create(serviceName, proto);
        services.remove(service, address);
        // single thread executor
        return scheduleExecutor.schedule(new ExceptionCatchingRunnable() {
            @Override
            public void runMayThrow() throws Exception {
                remove(serviceName, proto, address);
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    private class RefreshServices extends ExceptionCatchingRunnable {
        @Override
        public void runMayThrow() throws Exception {
            for (Map.Entry<Pair<String, String>, Collection<InetSocketAddress>> serviceEntry : services.asMap().entrySet()) {
                for (InetSocketAddress address : serviceEntry.getValue()) {
                    update(serviceEntry.getKey().left, serviceEntry.getKey().right, address);
                }
            }
        }
    }
}