package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cn.v5.lbrpc.common.client.core.PooledConnection.State.*;

/**
 * Created by wy96fyw@gmail.com on 2017/7/25.
 */
public class MultiConnectionPool<T extends IRequest, V extends IResponse> extends HostConnectionPool<T, V> {

    private final AtomicInteger open = new AtomicInteger(0);
    private final AtomicInteger scheduleCreation = new AtomicInteger(0);
    private List<PooledConnection> connections = new CopyOnWriteArrayList<>();
    private List<PooledConnection> trashedConnections = new CopyOnWriteArrayList<>();
    private Lock waitLock = new ReentrantLock();
    private Condition waitCondition = waitLock.newCondition();
    private volatile int waiters = 0;
    private Runnable newConnectionTask;
    private AtomicInteger totalInFlight = new AtomicInteger(0);
    private AtomicInteger maxTotalInFlight = new AtomicInteger(0);
    private static final int MAX_SIMULTANEOUS_CREATION = 1;

    protected MultiConnectionPool(Host host, NettyManager<T, V> nettyManager) throws ConnectionException {
        super(host, nettyManager);
        this.newConnectionTask = new Runnable() {
            @Override
            public void run() {
                addConnectionIfUnderMaximum();
                scheduleCreation.decrementAndGet();
            }
        };
        for (int i = 0; i < options().getCorePool(); i++) {
            scheduleCreation.getAndIncrement();
            nettyManager.blockingExecutor.execute(newConnectionTask);
        }
        awaitConnection(5, TimeUnit.SECONDS);
    }

    private PooledConnection tryResurrectTrashedConnection() {
        PooledConnection res = null;
        for (PooledConnection connection : trashedConnections) {
            if (connection.state.compareAndSet(TRASHED, RESSURECTING)) {
                res = connection;
                trashedConnections.remove(connection);
                break;
            }
        }
        return res;
    }

    private void addConnectionIfUnderMaximum() {
        for (; ; ) {
            if (open.get() > options().getMaxPool()) {
                return;
            }
            int current = open.get();
            if (open.compareAndSet(current, current + 1)) break;
        }
        PooledConnection newConnection = null;
        try {
            newConnection = tryResurrectTrashedConnection();
            if (newConnection == null) {
                newConnection = nettyManager.connectionFactory.open(this);
            }
            newConnection.state.compareAndSet(RESSURECTING, OPEN);
            connections.add(newConnection);

            if (isClosed() && !newConnection.isClosed()) {
                newConnection.closeAsync();
                open.decrementAndGet();
                return;
            }

            signalAvailableConnection(false);
        } catch (ConnectionException ce) {
            open.decrementAndGet();
            if (newConnection != null) {
                connections.remove(newConnection);
            }
        } catch (InterruptedException ie) {
            open.decrementAndGet();
            if (newConnection != null) {
                connections.remove(newConnection);
            }
        }
    }

    private PoolOptions options() {
        return nettyManager.configuration().getPoolOptions();
    }

    @Override
    PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (connections.isEmpty()) {
            if (options().getCorePool() == 0) {
                maybeSpawnNewConnection();
            } else {
                for (int i = 0; i < options().getCorePool(); i++) {
                    scheduleCreation.getAndIncrement();
                    nettyManager.blockingExecutor.execute(newConnectionTask);
                }
            }
            PooledConnection c = awaitConnection(timeout, unit);
            totalInFlight.getAndIncrement();
            return c;
        }
        PooledConnection leastBusy = null;
        int min = Integer.MAX_VALUE;
        for (PooledConnection connection : connections) {
            if (connection.inFlight.get() < min) {
                min = connection.inFlight.get();
                leastBusy = connection;
            }
        }
        if (leastBusy == null) {
            if (isClosed()) {
                throw new ConnectionException(host.getAddress(), "Connection pool closed");
            }
            leastBusy = awaitConnection(timeout, unit);
        } else {
            while (true) {
                int pending = leastBusy.inFlight.get();
                if (pending > options().getMaxRequestsPerConnection()) {
                    leastBusy = awaitConnection(timeout, unit);
                    break;
                }
                if (leastBusy.inFlight.compareAndSet(pending, pending + 1)) {
                    break;
                }
            }
        }
        int total = totalInFlight.getAndIncrement();
        int maxTotal = maxTotalInFlight.get();
        while (total > maxTotal) {
            if (maxTotalInFlight.compareAndSet(maxTotal, total)) {
                break;
            } else {
                maxTotal = maxTotalInFlight.get();
            }
        }
        int connectionCount = open.get() + scheduleCreation.get();
        if (connectionCount < options().getCorePool()) {
            maybeSpawnNewConnection();
        } else if (connectionCount < options().getMaxPool()) {
            int currentCapacity = (connectionCount - 1) * options().getMaxRequestsPerConnection()
                    + options().getNewConnectionThreshold();
            if (currentCapacity < total) {
                maybeSpawnNewConnection();
            }
        }
        return leastBusy;
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) throws InterruptedException {
        waitLock.lock();
        try {
            waiters++;
            waitCondition.await(timeout, unit);
        } finally {
            waiters--;
            waitLock.unlock();
        }
    }

    private PooledConnection awaitConnection(long timeout, TimeUnit unit) throws ConnectionException {
        if (timeout == 0) {
            throw new ConnectionException(host.getAddress(), "await connection timeout");
        }
        long remaining = timeout;
        long start = System.nanoTime();
        while (remaining > 0) {
            try {
                awaitAvailableConnection(remaining, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                timeout = 0;
            }
            if (isClosed()) {
                throw new ConnectionException(host.getAddress(), "Host connection pool is shutdown");
            }
            PooledConnection leastBusy = null;
            int min = Integer.MAX_VALUE;
            for (PooledConnection connection : connections) {
                if (connection.inFlight.get() < min) {
                    min = connection.inFlight.get();
                    leastBusy = connection;
                }
            }
            if (leastBusy != null) {
                while (true) {
                    int cur = leastBusy.inFlight.get();
                    if (cur >= options().getMaxRequestsPerConnection()) {
                        break;
                    } else if (leastBusy.inFlight.compareAndSet(cur, cur + 1)) {
                        return leastBusy;
                    }
                }
            }
            remaining = timeout - timeSince(start, unit);
        }
        throw new ConnectionException(host.getAddress(), "await connection timeout");
    }

    private void signalAvailableConnection(boolean all) {
        if (waiters != 0) {
            return;
        }
        waitLock.lock();
        if (all) {
            waitCondition.signalAll();
        } else {
            waitCondition.signal();
        }
        waitLock.unlock();
    }

    @Override
    void returnConnection(PooledConnection connection) {
        connection.inFlight.decrementAndGet();
        totalInFlight.decrementAndGet();

        if (isClosed()) {
            connection.closeAsync();
            return;
        }

        if (connection.state.get() != TRASHED) {
            signalAvailableConnection(false);
        }
    }

    private void maybeSpawnNewConnection() {
        if (isClosed()) {
            return;
        }
        int creating = scheduleCreation.get();
        while (true) {
            if (creating > MAX_SIMULTANEOUS_CREATION) {
                return;
            } else if (scheduleCreation.compareAndSet(creating, creating + 1)) {
                nettyManager.blockingExecutor.execute(newConnectionTask);
                break;
            } else {
                creating = scheduleCreation.get();
            }
        }
    }

    @Override
    CloseFuture makeCloseFuture() {
        signalAvailableConnection(true);
        List<CloseFuture> closeFutures = discardConnections();
        return new CloseFuture.Forwarding(closeFutures);
    }

    private List<CloseFuture> discardConnections() {
        List<CloseFuture> futures = new ArrayList<CloseFuture>();
        for (PooledConnection connection : connections) {
            if (connection != null) {
                CloseFuture future = connection.closeAsync();
                future.addListener(new Runnable() {
                    @Override
                    public void run() {
                        if (connection.state.compareAndSet(OPEN, GONE)) {
                            open.getAndDecrement();
                        }
                    }
                }, MoreExecutors.sameThreadExecutor());
                futures.add(future);
            }
        }

        for (PooledConnection connection : trashedConnections) {
            futures.add(connection.closeAsync());
        }
        return futures;
    }

    private boolean trashConnection(PooledConnection connection) {
        if (connection.state.compareAndSet(OPEN, TRASHED)) {
            while (true) {
                int opened = open.get();
                if (opened <= options().getCorePool()) {
                    connection.state.set(OPEN);
                    return false;
                }
                if (open.compareAndSet(opened, opened - 1)) {
                    break;
                }
            }
            connection.expireTime = System.currentTimeMillis() + options().getMaxIdleTime();
            trashedConnections.add(connection);
            connections.remove(connection);
            return true;
        } else {
            return false;
        }
    }

    private void cleanUpConnections() {
        for (PooledConnection connection : trashedConnections) {
            if (connection.inFlight.get() == 0 && connection.expireTime < System.currentTimeMillis()) {
                if (connection.state.compareAndSet(TRASHED, GONE)) {
                    connection.closeAsync();
                }
            }
        }
    }

    private void shrinkIfIdle() {
        if (isClosed()) {
            return;
        }

        int currentLoad = maxTotalInFlight.getAndSet(totalInFlight.get());
        int needed = currentLoad / options().getMaxRequestsPerConnection() + 1;
        if (currentLoad % options().getMaxRequestsPerConnection() > options().getNewConnectionThreshold()) {
            needed += 1;
        }
        needed = Math.max(needed, options().getCorePool());
        int opened = open.get();
        int trash = opened - needed;
        if (trash <= 0) {
            return;
        }
        for (PooledConnection connection : connections) {
            if (trashConnection(connection)) {
                trash--;
                if (trash == 0) break;
            }

        }
        cleanUpConnections();
    }
}
