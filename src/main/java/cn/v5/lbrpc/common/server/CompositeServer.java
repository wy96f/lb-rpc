package cn.v5.lbrpc.common.server;

import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yangwei on 15-6-15.
 */
public class CompositeServer {
    private static final Logger logger = LoggerFactory.getLogger(CompositeServer.class);

    private final Map<Integer, LifeCycleServer> serverMap = Maps.newHashMap();

    private final ServiceRegistration registration;

    private final List<Object> interceptors = new ArrayList<>();

    private final AtomicBoolean closing = new AtomicBoolean(false);

    // fail fast, don't register service if tomcat server starting failed
    static {
        System.setProperty("org.apache.catalina.startup.EXIT_ON_INIT_FAILURE", "true");
    }

    public CompositeServer(ServiceRegistration registration) {
        this(registration, new ServerInterceptor[0]);
    }

    public CompositeServer(ServiceRegistration registration, Object... interceptors) {
        this.interceptors.addAll(Arrays.asList(interceptors));
        this.registration = registration;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("Run shutdown hook of composite server now.");
                close();
                logger.info("Composite server shutdown completed");
            }
        }));
    }

    public void register(Object service, String proto) throws Exception {
        register(service, proto, -1);
    }

    public synchronized void register(Object service, String proto, int port) throws Exception {
        if (port == -1) {
            port = AbstractServerFactory.newFactory(proto).getDefaultPort();
        }
        LifeCycleServer server = serverMap.get(port);
        if (server == null) {
            InetAddress address = InetAddress.getLocalHost();
            server = AbstractServerFactory.newFactory(proto).withRegistration(registration).withAddress(address).withPort(port).createServer(interceptors);
            serverMap.put(port, server);
            server.start();
            server.register(service, "");
        } else if (server.getProto().compareTo(proto) == 0) {
            server.register(service, "");
        } else {
            throw new IllegalArgumentException(String.format("protocol %s has already been running in port %d now", server.getProto(), port));
        }
    }

    @VisibleForTesting
    public synchronized void unregister(String proto) {
        unregister(proto, -1);
    }

    @VisibleForTesting
    public synchronized void unregister(String proto, int port) {
        if (port == -1) {
            port = AbstractServerFactory.newFactory(proto).getDefaultPort();
        }
        LifeCycleServer server = serverMap.get(port);
        if (server == null) {
            return;
        }
        server.unregister();
        server.stop();
        serverMap.remove(port);
    }

    public void close() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        for (Map.Entry<Integer, LifeCycleServer> serverEntry : serverMap.entrySet()) {
            logger.info("close server listening at {} with supported proto {}", serverEntry.getKey(), serverEntry.getValue().getProto());
            serverEntry.getValue().unregister();
            serverEntry.getValue().stop();
        }
        serverMap.clear();
    }
}