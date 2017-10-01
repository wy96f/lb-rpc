package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yangwei on 15-5-5.
 */
public class PooledConnection<Request extends IRequest, Response extends IResponse> extends Connection<Request, Response> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultResultFuture.class);

    final HostConnectionPool pool;

    final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

    long expireTime;

    PooledConnection(String name, InetSocketAddress address, IPipelineAndHeartbeat<Request, Response> pipelineAndHearbeat,
                     Factory factory, HostConnectionPool pool) throws ConnectionException {
        super(name, address, pipelineAndHearbeat, factory);
        this.pool = pool;
    }

    void release() {
        if (pool == null) {
            logger.debug("the pool {} is null", this);
            return;
        }

        pool.returnConnection(this);
    }

    @Override
    protected void
    notifyOwnerWhenDefunct(boolean hostIsDown) {
        // This can happen if an exception is thrown at construction time. In
        // that case the pool will handle it itself.
        if (pool == null)
            return;

        pool.closeAsync().force();
    }

    enum State {
        OPEN, TRASHED, RESSURECTING, GONE;
    }
}
