package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yangwei on 15-5-14.
 */
public class CommonConnectionPool extends HostConnectionPool {
    private final GenericObjectPool<PooledConnection> pool;

    protected CommonConnectionPool(Host host, NettyManager nettyManager) {
        super(host, nettyManager);
        this.pool = new GenericObjectPool<PooledConnection>(new ConnectionPoolFactory(host, nettyManager));
        pool.setMaxIdle(4);
        pool.setMaxTotal(options().getMaxPool());
        pool.setMaxWaitMillis(options().getPoolTimeoutMs());
        pool.setMinEvictableIdleTimeMillis(10000);
        pool.setTestOnBorrow(true);
        pool.setTestOnReturn(true);
        pool.setTimeBetweenEvictionRunsMillis(20000);
    }


    private PoolOptions options() {
        return nettyManager.configuration().getPoolOptions();
    }

    @Override
    PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (isClosed())
            // Note: throwing a ConnectionException is probably fine in practice as it will trigger the creation of a new host.
            // That being said, maybe having a specific exception could be cleaner.
            throw new ConnectionException(host.getAddress(), "Pool is shutdown");

        try {
            return pool.borrowObject(TimeUnit.MILLISECONDS.convert(timeout, unit));
        } catch (NoSuchElementException e) {
            if (e.getMessage().contains("Timeout waiting for"))
                throw new TimeoutException();
            else
                throw new ConnectionException(host.getAddress(), e, e.getMessage());
        } catch (Exception e) {
            throw new ConnectionException(host.getAddress(), e, e.getMessage());
        }
    }

    @Override
    void returnConnection(PooledConnection connection) {
        if (isClosed()) {
            connection.closeAsync();
        }
        if (connection.isDefunct()) {
            // As part of making it defunct, we have already replaced it or
            // closed the pool.
            return;
        }
        pool.returnObject(connection);
    }

    @Override
    CloseFuture makeCloseFuture() {
        if (pool != null) {
            pool.clear();
            pool.close();
        }
        return CloseFuture.immediateFuture();
    }

    private class ConnectionPoolFactory implements PooledObjectFactory<PooledConnection> {
        private final Host host;
        private final NettyManager nettyManager;

        private ConnectionPoolFactory(Host host, NettyManager nettyManager) {
            this.host = host;
            this.nettyManager = nettyManager;
        }

        @Override
        public PooledObject<PooledConnection> makeObject() throws Exception {
            PooledConnection connection = nettyManager.connectionFactory.open(CommonConnectionPool.this);
            return new DefaultPooledObject<PooledConnection>(connection);
        }

        @Override
        public void destroyObject(PooledObject<PooledConnection> p) throws Exception {
            p.getObject().closeAsync().force();
        }

        @Override
        public boolean validateObject(PooledObject<PooledConnection> p) {
            return !p.getObject().isClosed() && !p.getObject().isDefunct();
        }

        @Override
        public void activateObject(PooledObject<PooledConnection> p) throws Exception {

        }

        @Override
        public void passivateObject(PooledObject<PooledConnection> p) throws Exception {

        }
    }
}
