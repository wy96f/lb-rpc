package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * Created by yangwei on 15-5-8.
 */
class NettyManager<T extends IRequest, V extends IResponse> extends AbstractManager<T, V> {
    private static final Logger logger = LoggerFactory.getLogger(NettyManager.class);
    final ConcurrentMap<Host, HostConnectionPool> pools;
    final Connection.Factory<T, V> connectionFactory;
    final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();
    final ScheduledThreadPoolExecutor reconnectionExecutor = new ScheduledThreadPoolExecutor(2, threadFactory("Netty-Reconnection-%d"));
    final ConnectionReaper reaper;
    private final String MANAGER_PREFIX = "Netty";

    NettyManager(IPipelineAndHeartbeat<T, V> pipelineAndHearbeat, Configuration configuration) {
        super(configuration);
        this.pools = new ConcurrentHashMap<Host, HostConnectionPool>();

        this.connectionFactory = new Connection.Factory<T, V>(this, pipelineAndHearbeat, configuration);

        this.reaper = new ConnectionReaper();
    }

    protected static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    @Override
    public String getPrefix() {
        return MANAGER_PREFIX;
    }

    @Override
    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    public CloseFuture close() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        logger.debug("Shutting down");

        // If we're shutting down, there is no point in waiting on scheduled reconnections, nor on notifications
        // delivery or blocking tasks so we use shutdownNow
        shutdownNow(reconnectionExecutor);
        shutdownNow(blockingExecutor);

        // but for the worker executor, we want to let submitted tasks finish unless the shutdown is forced.
        executor.shutdown();

        isClosing = true;
        List<CloseFuture> futures = new ArrayList<CloseFuture>();
        for (HostConnectionPool pool : pools.values())
            futures.add(pool.closeAsync());

        // The rest will happen asynchronously, when all connections are successfully closed
        future = new ManagerCloseFuture(futures);
        return closeFuture.compareAndSet(null, future)
                ? future
                : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    Configuration configuration() {
        return configuration;
    }

    @Override
    public CloseFuture removePool(Host host) {
        final HostConnectionPool pool = pools.remove(host);
        return pool == null ? CloseFuture.immediateFuture()
                : pool.closeAsync();
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

                        HostConnectionPool newPool = HostConnectionPool.newInstance(host, NettyManager.this);

                        HostConnectionPool previous = pools.put(host, newPool);
                        if (previous == null) {
                            logger.debug("Added connection pool for {}", host);
                        } else {
                            logger.debug("Renewed connection pool for {}", host);
                            previous.closeAsync();
                        }

                        if (isClosing) {
                            newPool.closeAsync();
                            pools.remove(newPool);
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

    @Override
    public ListenableFuture<Boolean> maybeAddPool(final Host host, ListeningExecutorService executor) {
        HostConnectionPool previous = pools.get(host);
        if (previous != null && !previous.isClosed())
            return Futures.immediateFuture(true);

        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    while (true) {
                        HostConnectionPool previous = pools.get(host);
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

    private boolean replacePool(Host host, HostConnectionPool condition) throws ConnectionException {
        if (isClosing) {
            return false;
        }
        Lock l = poolCreationLocks.get(host);
        l.lock();
        try {
            HostConnectionPool previous = pools.get(host);
            if (previous != condition)
                return false;

            HostConnectionPool newPool = HostConnectionPool.newInstance(host, this);

            previous = pools.put(host, newPool);
            if (previous != null && !previous.isClosed()) {
                logger.warn("Replacing a pool that wasn't closed. Closing it now, but this was not expected.");
                previous.closeAsync();
            }
            if (isClosing) {
                newPool.closeAsync();
                pools.remove(newPool);
                return false;
            }
            return true;
        } finally {
            l.unlock();
        }
    }

    boolean signalConnectionFailure(Host host, ConnectionException exception, boolean isHostAddition) {
        if (isClosed()) {
            return true;
        }

        triggerOnDown(host, isHostAddition, true);
        return true;
    }

    public void startPeriodicReconnectionAttempt(final Host host, final boolean isHostAddition) {
        new AbstractReconnectionHandler(reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt) {
            @Override
            public IPrimeConnection tryReconnect() throws ConnectionException, InterruptedException {
                return connectionFactory.open(host);
            }

            @Override
            public void onReconnection(IPrimeConnection connection) {
                connection.closeAsync();

                logger.debug("Successful reconnection to {}, setting host UP", host);
                try {
                    if (isHostAddition)
                        onAdd(host);
                    else
                        onUp(host);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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
                triggerOnRemoval(host);
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
                triggerOnRemoval(host);
                return true;
            }
        }.start();
    }

    static class ConnectionReaper {
        private static final int INTERVAL_MS = 15000;

        private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, threadFactory("Reaper-%d"));
        private final Map<Connection, Long> connections = new ConcurrentHashMap<Connection, Long>();
        private final Runnable reaperTask = new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Iterator<Map.Entry<Connection, Long>> iterator = connections.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Connection, Long> entry = iterator.next();
                    Connection connection = entry.getKey();
                    Long terminateTime = entry.getValue();
                    if (terminateTime <= now) {
                        boolean terminated = connection.tryTerminate(true);
                        if (terminated)
                            iterator.remove();
                    }
                }
            }
        };
        private volatile boolean shutdown;

        ConnectionReaper() {
            executor.scheduleWithFixedDelay(reaperTask, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        void register(Connection connection, long terminateTime) {
            if (shutdown) {
                // This should not happen since the reaper is shut down after all sessions.
                logger.warn("Connection registered after reaper shutdown: {}", connection);
                connection.tryTerminate(true);
            } else {
                connections.put(connection, terminateTime);
            }
        }

        void shutdown() {
            shutdown = true;
            // Force shutdown to avoid waiting for the interval, and run the task manually one last time
            executor.shutdownNow();
            reaperTask.run();
        }
    }

    private class ManagerCloseFuture extends CloseFuture.Forwarding {
        ManagerCloseFuture(List<CloseFuture> futures) {
            super(futures);
        }

        @Override
        public CloseFuture force() {
            // The only ExecutorService we haven't forced yet is executor
            shutdownNow(executor);
            return super.force();
        }

        @Override
        protected void onFuturesDone() {
                /*
                 * When we reach this, all sessions should be shutdown. We've also started a shutdown
                 * of the thread pools used by this object. Remains 2 things before marking the shutdown
                 * as done:
                 *   1) we need to wait for the completion of the shutdown of the Cluster threads pools.
                 *   2) we need to shutdown the Connection.Factory, i.e. the executors used by Netty.
                 * But at least for 2), we must not do it on the current thread because that could be
                 * a netty worker, which we're going to shutdown. So creates some thread for that.
                 */
            (new Thread("Shutdown-checker") {
                @Override
                public void run() {
                    // Just wait indefinitely on the the completion of the thread pools. Provided the user
                    // call force(), we'll never really block forever.
                    try {
                        reconnectionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                        blockingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

                        // Some of the jobs on the executors can be doing query stuff, so close the
                        // connectionFactory at the very last
                        connectionFactory.shutDown();

                        reaper.shutdown();

                        set(null);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        setException(e);
                    }
                }
            }).start();
        }
    }
}
