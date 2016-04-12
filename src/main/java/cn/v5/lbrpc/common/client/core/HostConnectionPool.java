package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yangwei on 15-5-3.
 */
public abstract class HostConnectionPool<T extends IRequest, V extends IResponse> {
    protected final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();
    final Host host;
    final NettyManager<T, V> nettyManager;
    protected HostConnectionPool(Host host, NettyManager<T, V> nettyManager) {
        this.host = host;
        this.nettyManager = nettyManager;
    }

    public static <T extends IRequest, V extends IResponse> HostConnectionPool newInstance(Host host, NettyManager<T, V> nettyManager) throws ConnectionException {
        return new SingleConnectionPool<T, V>(host, nettyManager);
        //return new CommonConnectionPool(host, nettyManager);
    }

    static long timeSince(long startNanos, TimeUnit destUnit) {
        return destUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    abstract PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException;

    abstract void returnConnection(PooledConnection connection);

    abstract CloseFuture makeCloseFuture();

    final boolean isClosed() {
        return closeFuture.get() != null;
    }

    final CloseFuture closeAsync() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        future = makeCloseFuture();

        return closeFuture.compareAndSet(null, future) ?
                future : closeFuture.get();
    }
}
