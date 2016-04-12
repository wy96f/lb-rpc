package cn.v5.lbrpc.http.client.core;

import cn.v5.lbrpc.common.client.core.*;
import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient4Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * Created by yangwei on 15-6-8.
 */
public class HttpManager extends AbstractManager {
    private static final Logger logger = LoggerFactory.getLogger(HttpManager.class);
    final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();
    private final String MANAGER_PREFIX = "Http";
    Map<Host, ResteasyClient> clients;

    public HttpManager(Configuration configuration) {
        super(configuration);
        this.clients = new ConcurrentHashMap<Host, ResteasyClient>();
    }

    @Override
    public String getPrefix() {
        return MANAGER_PREFIX;
    }

    @Override
    public ListenableFuture<Boolean> maybeAddPool(final Host host, ListeningExecutorService executor) {
        ResteasyClient previous = clients.get(host);
        if (previous != null && !previous.isClosed())
            return Futures.immediateFuture(true);

        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    while (true) {
                        ResteasyClient previous = clients.get(host);
                        if (previous != null && !previous.isClosed())
                            return true;

                        if (replacePool(host, previous)) {
                            logger.debug("Added connection pool for {}", host);
                            return true;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error creating pool to " + host, e);
                    return false;
                }
            }
        });
    }

    private boolean replacePool(Host host, ResteasyClient condition) throws ConnectionException {
        if (isClosing) {
            return false;
        }
        Lock l = (Lock) poolCreationLocks.get(host);
        l.lock();
        try {
            ResteasyClient previous = clients.get(host);
            if (previous != condition)
                return false;

            ResteasyClient newPool = createClientPool();

            previous = clients.put(host, newPool);
            if (previous != null && !previous.isClosed()) {
                logger.warn("Replacing a pool that wasn't closed. Closing it now, but this was not expected.");
                previous.close();
            }
            if (isClosing) {
                newPool.close();
                clients.remove(newPool);
                return false;
            }
            return true;
        } finally {
            l.unlock();
        }
    }

    @Override
    public ListenableFuture<Boolean> forceRenewPool(final Host host, ListeningExecutorService executor) {
        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                while (true) {
                    try {
                        if (isClosing)
                            return true;

                        ResteasyClient newPool = createClientPool();

                        ResteasyClient previous = clients.put(host, newPool);
                        if (previous == null) {
                            logger.debug("Added connection pool for {}", host);
                        } else {
                            logger.debug("Renewed connection pool for {}", host);
                            previous.close();
                        }

                        if (isClosing) {
                            newPool.close();
                            clients.remove(newPool);
                        }

                        return true;
                    } catch (Exception e) {
                        logger.error("Error creating pool to " + host, e);
                        return false;
                    }
                }
            }
        });
    }

    private ResteasyClient createClientPool() {
        int connectionTimeout = configuration.getSocketOptions().getConnectionTimeoutMillis();
        int connectionCheckoutTimeout = configuration.getPoolOptions().getPoolTimeoutMs();
        int maxPool = configuration.getPoolOptions().getMaxPool();
        int readTimeout = configuration.getSocketOptions().getReadTimeoutMillis();
        return new ResteasyClientBuilder()
                .establishConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .connectionPoolSize(maxPool)
                .connectionCheckoutTimeout(connectionCheckoutTimeout, TimeUnit.MILLISECONDS)
                .socketTimeout(readTimeout, TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public CloseFuture removePool(Host host) {
        final ResteasyClient pool = clients.remove(host);
        if (pool != null) {
            pool.close();
        }
        return CloseFuture.immediateFuture();
    }

    @Override
    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    @Override
    public CloseFuture close() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        logger.debug("Shutting down");

        // If we're shutting down, there is no point in waiting on scheduled reconnections, nor on notifications
        // delivery or blocking tasks so we use shutdownNow
        shutdownNow(reconnectionExecutor);
        shutdownNow(blockingExecutor);

        shutdownNow(executor);

        isClosing = true;

        for (ResteasyClient client : clients.values()) {
            client.close();
        }

        future = CloseFuture.immediateFuture();

        return closeFuture.compareAndSet(null, future)
                ? future
                : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    public void startPeriodicReconnectionAttempt(final Host host, final boolean isHostAddition) {
        new AbstractReconnectionHandler(reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt) {
            @Override
            public IPrimeConnection tryReconnect() throws ConnectionException, InterruptedException {
                return new HttpPrimeConnection(host).connect();
            }

            @Override
            public void onReconnection(IPrimeConnection connection) {
                // We don't use that first connection so close it.
                // we use it for the first HostConnectionPool created
                connection.closeAsync();

                logger.debug("Successful reconnection to {}, setting host UP", host);
                try {
                    if (isHostAddition)
                        onAdd(host);
                    else
                        onUp(host);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    logger.error("Unexpected error while setting node up", e);
                }
            }

            @Override
            public boolean onConnectionException(ConnectionException e, int retryCount, long elapsedTime, long nextDelayMs) {
                if (!reconnectionPolicy().retry(e, retryCount, elapsedTime)) {
                    logger.debug("Failed reconnection to {} ({}), stop reconnection after {} times and {} ms",
                            host, e.getMessage(), retryCount, elapsedTime);
                    return false;
                }
                if (logger.isDebugEnabled())
                    logger.debug("Failed reconnection to {} ({}), scheduling retry in {} milliseconds", host, e.getMessage(), nextDelayMs);
                return true;
            }

            @Override
            public boolean onUnknownException(Exception e, int retryCount, long elapsedTime, long nextDelayMs) {
                if (!reconnectionPolicy().retry(e, retryCount, elapsedTime)) {
                    logger.error(String.format("Unknown error during reconnection to %s, stop reconnection after %d times and %d ms",
                            host, retryCount, elapsedTime), e);
                    return false;
                }
                logger.error(String.format("Unknown error during reconnection to %s, scheduling retry in %d milliseconds", host, nextDelayMs), e);
                return true;
            }
        }.start();
    }
}