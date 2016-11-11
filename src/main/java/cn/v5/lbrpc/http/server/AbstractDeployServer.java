package cn.v5.lbrpc.http.server;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import cn.v5.lbrpc.common.server.IServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.http.utils.HttpUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yangwei on 15-6-6.
 */
public abstract class AbstractDeployServer implements IServer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDeployServer.class);
    final InetSocketAddress address;
    private List<Object> interceptors;
    private final ServiceRegistration registration;
    private final Set<String> services = Sets.newCopyOnWriteArraySet();

    protected AbstractDeployServer(InetSocketAddress address, List<Object> interceptors, ServiceRegistration registration) {
        this.address = address;
        this.interceptors = interceptors;
        this.registration = registration;
    }

    @Override
    public void start() {
        getDeployment().getMediaTypeMappings().put("json", "application/json");
        doStart();
        if (interceptors != null && !interceptors.isEmpty()) {
            for (Object interceptor : interceptors) {
                getDeployment().getProviderFactory().register(interceptor);
            }
        }
    }

    @Override
    public void register(Object instance, String contextPath) {
        if (contextPath == null || contextPath.isEmpty()) {
            getDeployment().getRegistry().addSingletonResource(instance);
        } else {
            getDeployment().getRegistry().addSingletonResource(instance, contextPath);
        }
        String serviceName = HttpUtils.getServiceName(instance.getClass());
        registration.registerServer(serviceName, CBUtil.HTTP_PROTO, address);
        services.add(serviceName);
    }

    @Override
    public void unregister() {
        List<ListenableFuture<?>> futureList = Lists.newArrayList();
        for (String service : services) {
            ListenableFuture<?> unregisterFuture = registration.unregisterServer(service, CBUtil.HTTP_PROTO, address);
            if (unregisterFuture != null) {
                futureList.add(unregisterFuture);
            }
        }
        try {
            Futures.successfulAsList(futureList).get(4000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Unexpected error while unregistering thrift services", e.getCause());
        } catch (TimeoutException e) {
            logger.warn("Time out while unregistering thrift services", e);
        }
    }

    protected abstract void doStart();

    protected abstract ResteasyDeployment getDeployment();
}