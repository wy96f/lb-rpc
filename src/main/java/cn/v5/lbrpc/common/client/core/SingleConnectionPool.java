package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yangwei on 15-5-5.
 */
public class SingleConnectionPool<T extends IRequest, V extends IResponse> extends HostConnectionPool<T, V> {
    private static final Logger logger = LoggerFactory.getLogger(SingleConnectionPool.class);

    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();
    private final Runnable newConnectionTask;
    private final AtomicBoolean open = new AtomicBoolean();
    private final AtomicBoolean scheduledForCreation = new AtomicBoolean();
    volatile AtomicReference<PooledConnection> connectionRef = new AtomicReference<PooledConnection>();
    private volatile int waiter = 0;

    SingleConnectionPool(Host host, NettyManager<T, V> nettyManager) throws ConnectionException {
        super(host, nettyManager);

        this.newConnectionTask = new Runnable() {
            @Override
            public void run() {
                addConnectionIfNeeded();
                scheduledForCreation.set(false);
            }
        };

        try {
            connectionRef.set(nettyManager.connectionFactory.open(this));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // If asked to interrupt, we can skip opening core connections, the pool will still work.
            // But we ignore otherwise cause I'm not sure we can do much better currently.
        }

        this.open.set(true);

        logger.trace("Created connection pool to host {}", host);
    }

    @Override
    PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (isClosed()) {
            // Note: throwing a ConnectionException is probably fine in practice as it will trigger the creation of a new host.
            // That being said, maybe having a specific exception could be cleaner.
            throw new ConnectionException(host.getAddress(), "Pool is shutdown");
        }

        PooledConnection connection = connectionRef.get();
        if (connection == null) {
            if (scheduledForCreation.compareAndSet(false, true))
                nettyManager.blockingExecutor.submit(newConnectionTask);
            connection = waitForConnection(timeout, TimeUnit.SECONDS);
        } else {
        }
        return connection;
    }

    @Override
    void returnConnection(PooledConnection connection) {
        if (isClosed()) {
            close(connection);
        }

        if (connection.isDefunct()) {
            return;
        }

        signalAvailableConnection();
    }

    @Override
    CloseFuture makeCloseFuture() {
        signalAvailableConnection();
        return new CloseFuture.Forwarding(discardConnection());
    }

    private List<CloseFuture> discardConnection() {
        List<CloseFuture> futures = new ArrayList<CloseFuture>();

        final PooledConnection connection = connectionRef.get();
        if (connection != null) {
            CloseFuture future = connection.closeAsync();
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    open.set(false);
                }
            }, MoreExecutors.sameThreadExecutor());
            futures.add(future);
        }
        return futures;
    }

    private void close(final Connection connection) {
        connection.closeAsync();
    }

    private boolean addConnectionIfNeeded() {
        if (!open.compareAndSet(false, true))
            return false;

        if (isClosed()) {
            open.set(false);
            return false;
        }

        try {
            connectionRef.set(nettyManager.connectionFactory.open(this));
            signalAvailableConnection();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            open.set(false);
            return false;
        } catch (ConnectionException e) {
            open.set(false);
            logger.debug("Connection error to {} while creating additional connection", host);
            return false;
        }
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) throws InterruptedException {
        waitLock.lock();
        waiter++;
        try {
            hasAvailableConnection.await(timeout, unit);
        } finally {
            waiter--;
            waitLock.unlock();
        }
    }

    private void signalAvailableConnection() {
        if (waiter == 0) {
            return;
        }
        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        } finally {
            waitLock.unlock();
        }
    }


    private PooledConnection waitForConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (timeout == 0)
            throw new TimeoutException();

        long start = System.nanoTime();
        long remaining = timeout;
        do {
            try {
                awaitAvailableConnection(remaining, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                timeout = 0;
            }

            if (isClosed())
                throw new ConnectionException(host.getAddress(), "Pool is shutdown");

            PooledConnection connection = connectionRef.get();
            if (connection != null) {
                return connection;
            }

            remaining = timeout - timeSince(start, unit);
        } while (remaining > 0);

        throw new TimeoutException();
    }

}
