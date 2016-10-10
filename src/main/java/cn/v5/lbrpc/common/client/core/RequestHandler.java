package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yangwei on 15-5-4.
 */
// TODO use adapter pattern to wrap onException, onTimeout, onSet, etc?
public class RequestHandler<Request extends IRequest, Response extends IResponse> implements Connection.ResponseCallback<Request, Response> {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private final NettyManager<Request, Response> nettyManager;
    private ResultSetCallback<Request, Response> callback;
    private final Iterator<Host> queryPlan;

    private final AtomicReference<QueryState> queryStateRef = new AtomicReference<QueryState>(QueryState.INITIAL);

    private volatile Host current;

    private volatile Connection.ResponseHandler responseHandler;

    private volatile Map<InetSocketAddress, Throwable> errors;

    // only for init use
    public RequestHandler(AbstractNodeClient<Request, Response> abstractNodeClient) {
        this.nettyManager = (NettyManager) abstractNodeClient.getManager();
        this.callback = null;
        this.queryPlan = null;
    }

    public RequestHandler(AbstractNodeClient<Request, Response> abstractNodeClient, Pair<String, String> serviceAndProto) {
        this.nettyManager = (NettyManager) abstractNodeClient.getManager();


        this.queryPlan = nettyManager.getLoadBalancingPolicy(serviceAndProto).queryPlan();
    }

    public void init(Pair<String, String> serviceAndProto, List<InetSocketAddress> initContactPoints, LoadBalancingPolicy loadBalancingPolicy) {
        nettyManager.addLoadBalancingPolicy(serviceAndProto, loadBalancingPolicy);
        nettyManager.init(serviceAndProto, initContactPoints);
    }


    public void sendRequest(ResultSetCallback callback) {
        this.callback = callback;
        callback.register(this);

        send();
    }

    public void send() {
        try {
            while (queryPlan.hasNext()) {
                Host host = queryPlan.next();
                if (logger.isTraceEnabled())
                    logger.trace("Querying node {}", host);
                if (query(host))
                    return;
            }
            setFinalException(null, new NoHostAvailableException(errors == null ? Collections.<InetSocketAddress, Throwable>emptyMap() : errors));
        } catch (Exception e) {
            setFinalException(null, new RpcInternalError("An unexpected error happened while sending requests", e));
        }
    }

    @Override
    public int retryCount() {
        return queryStateRef.get().retryCount;
    }

    private void setFinalException(Connection connection, Exception exception) {
        callback.onClose(errors, retryCount(), exception);
        callback.onException(connection, exception, retryCount());
    }

    private boolean query(Host host) {
        HostConnectionPool curPool = nettyManager.pools.get(host);
        if (curPool == null || curPool.isClosed()) {
            return false;
        }

        PooledConnection connection = null;
        try {
            connection = curPool.borrowConnection(nettyManager.configuration().getPoolOptions().getPoolTimeoutMs(), TimeUnit.MILLISECONDS);
            current = host;
            write(connection, this);
            return true;
        } catch (ConnectionException e) {
            if (connection != null)
                connection.release();
            logError(host.getAddress(), e);
            return false;
        } catch (TimeoutException e) {
            logError(host.getAddress(), e);
            return false;
        } catch (RuntimeException e) {
            if (connection != null)
                connection.release();
            logger.error("Unexpected error while querying " + host.getAddress().getAddress(), e);
            logError(host.getAddress(), e);
            return false;
        }
    }

    private void write(Connection connection, Connection.ResponseCallback<Request, Response> responseCallback) throws ConnectionException {
        responseHandler = null;

        // Ensure query state is "in progress" (can be already if connection.write failed on a previous node and we're retrying)
        while (true) {
            QueryState previous = queryStateRef.get();
            if (previous.inProgress || queryStateRef.compareAndSet(previous, previous.startNext())) {
                break;
            }
        }

        responseHandler = connection.write(responseCallback, false);
        // Only start the timeout when we're sure connectionHandler is set. This avoids an edge case where onTimeout() was triggered
        // *before* the call to connection.write had returned.
        responseHandler.startTimeout();
    }

    public void logError(InetSocketAddress address, Throwable exception) {
        logger.debug("Error querying {}, trying next host (error is: {})", address, exception.toString());
        if (errors == null)
            errors = new HashMap<InetSocketAddress, Throwable>();
        errors.put(address, exception);
    }

    @Override
    public Request request() {
        return callback.request();
    }

    public Map<InetSocketAddress, Throwable> getErrors() {
        return errors;
    }

    void retry(final boolean retryCurrent) {
        final Host h = current;
        nettyManager.executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (retryCurrent) {
                        if (query(h))
                            return;
                    }
                    send();
                } catch (Exception e) {
                    setFinalException(null, new RpcInternalError("Unexpected exception while retrying query", e));
                }
            }
        });
    }

    @Override
    public void onSet(Connection connection, Response response, int retryCount) {
        //logger.trace("on set request {}", response.getStreamId());
        QueryState queryState = queryStateRef.get();
        if (!queryState.isInProgressAt(retryCount) ||
                !queryStateRef.compareAndSet(queryState, queryState.complete())) {
            logger.debug("onSet triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                    retryCount, queryState, queryStateRef.get());
            return;
        }

        Host queriedHost = current;
        try {
            setFinalResult(connection, response);
        } catch (Exception e) {
            setFinalException(connection, e);
        } finally {
            if (connection instanceof PooledConnection) {
                ((PooledConnection) connection).release();
            }
        }
    }

    private void setFinalResult(Connection connection, Response response) {
        try {
            callback.onClose(errors, retryCount(), null);
            boolean retry = callback.onSet(connection, response);
            if (retry) {
                retry(false);
            }
        } catch (Exception e) {
            setFinalException(connection, new RpcInternalError("Unexpected exception while setting final result from " + response, e));
        }
    }

    @Override
    public void onException(Connection connection, Exception exception, int retryCount) {
        QueryState queryState = queryStateRef.get();
        if (!queryState.isInProgressAt(retryCount) ||
                !queryStateRef.compareAndSet(queryState, queryState.complete())) {
            logger.debug("onExcpetion triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                    retryCount, queryState, queryStateRef.get());
            return;
        }

        Host queriedHost = current;
        try {
            if (connection instanceof PooledConnection) {
                ((PooledConnection) connection).release();
            }

            if (exception instanceof ConnectionException) {
                ConnectionException ce = (ConnectionException) exception;
                logError(ce.address, ce);
                retry(false);
                return;
            }
            setFinalException(connection, exception);
        } catch (Exception e) {
            setFinalException(null, new RpcInternalError("An unexpected error happened while handing exception " + exception, e));
        }
    }

    @Override
    public boolean onTimeout(Connection connection, int streamId, int retryCount) {
        //logger.trace("on timeout request {}", streamId);
        QueryState queryState = queryStateRef.get();
        if (!queryState.isInProgressAt(retryCount) ||
                !queryStateRef.compareAndSet(queryState, queryState.complete())) {
            logger.debug("onTimeout triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                    retryCount, queryState, queryStateRef.get());
            return false;
        }

        Host queriedHost = current;
        try {
            RpcException timeoutException = new RpcException("Timed out waiting for server response");
            connection.defunct(timeoutException);

            logError(connection.address, timeoutException);
            retry(false);
        } catch (Exception e) {
            setFinalException(null, new RpcInternalError("An unexpected error happened while handing timeout", e));
        }
        return true;
    }

    public interface ResultSetCallback<Request extends IRequest, Response extends IResponse> {
        public Request request();
        public void register(RequestHandler handler);
        /**
         * @param connection
         * @param response
         * @return whether or not retry
         */
        public boolean onSet(Connection connection, Response response);
        public void onException(Connection connection, Exception exception, int retryCount);
        public void onClose(Map<InetSocketAddress, Throwable> error, int retryCount, Exception e);
    }

    static class QueryState {
        static final QueryState INITIAL = new QueryState(-1, false);

        final int retryCount;
        final boolean inProgress;

        QueryState(int retryCount, boolean inProgress) {
            this.retryCount = retryCount;
            this.inProgress = inProgress;
        }

        boolean isInProgressAt(int retryCount) {
            return inProgress && this.retryCount == retryCount;
        }

        QueryState startNext() {
            assert !inProgress;
            return new QueryState(retryCount + 1, true);
        }

        QueryState complete() {
            assert inProgress;
            return new QueryState(retryCount, false);
        }

        @Override
        public String toString() {
            return String.format("QueryState(count=%d, inProgress=%s)", retryCount, inProgress);
        }
    }
}
