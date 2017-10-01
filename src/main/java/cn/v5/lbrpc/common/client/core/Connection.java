package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.TransportException;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.StreamIdGenerator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yangwei on 15-5-3.
 */
public class Connection<Request extends IRequest, Response extends IResponse> implements IPrimeConnection {
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    public final InetSocketAddress address;
    private final Channel channel;
    private final Factory factory;

    private final Dispatcher dispatcher;
    private final String name;
    private final AtomicInteger writer = new AtomicInteger(0);
    private final AtomicReference<ConnectionCloseFuture> closeFuture = new AtomicReference<ConnectionCloseFuture>();
    private final IPipelineAndHeartbeat<Request, Response> pipelineAndHearbeat;
    private volatile boolean isInitialized;
    private volatile boolean isDefunct;

    protected AtomicInteger inFlight = new AtomicInteger();

    Connection(String name, InetSocketAddress address, IPipelineAndHeartbeat<Request, Response> pipelineAndHearbeat, Factory factory) throws ConnectionException {
        this.name = name;
        this.address = address;
        this.factory = factory;
        this.pipelineAndHearbeat = pipelineAndHearbeat;
        this.dispatcher = new Dispatcher();

        try {
            Bootstrap bootstrap = factory.newBootstrap();
            bootstrap.handler(new Initializer(this, pipelineAndHearbeat));
            ChannelFuture future = bootstrap.connect(address);

            writer.incrementAndGet();
            try {
                this.channel = future.awaitUninterruptibly().channel();
                if (!future.isSuccess()) {
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("%s Error connecting to %s%s", this, address, future.cause()));
                    throw defunct(new TransportException(address, future.cause(), "Cannot connect"));
                }
            } finally {
                writer.decrementAndGet();
            }

            logger.debug("{} Connection opened successfully", this);
        } catch (ConnectionException e) {
            throw e;
        }
        isInitialized = true;
    }

    ResponseHandler write(ResponseCallback<Request, Response> callback, boolean startTimeout) throws ConnectionException {
        Request request = callback.request();

        ResponseHandler handler = new ResponseHandler(this, callback);
        dispatcher.add(handler);
        request.setStreamId(handler.streamId);

         /*
         * We check for close/defunct *after* having set the handler because closing/defuncting
         * will set their flag and then error out handler if need. So, by doing the check after
         * having set the handler, we guarantee that even if we race with defunct/close, we may
         * never leave a handler that won't get an answer or be errored out.
         */
        if (isDefunct) {
            dispatcher.removeHandler(handler, true);
            throw new ConnectionException(address, "Write attempt on defunct connection");
        }

        if (isClosed()) {
            dispatcher.removeHandler(handler, true);
            throw new ConnectionException(address, "Connection has been closed");
        }

        logger.debug("{} writing request {}", this, request);
        writer.incrementAndGet();
        channel.writeAndFlush(request).addListener(writeHandler(request, handler));

        if (startTimeout) {
            handler.startTimeout();
        }

        return handler;
    }

    boolean isDefunct() {
        return isDefunct;
    }

    protected void notifyOwnerWhenDefunct(boolean hostIsDown) {
    }

    public <E extends Exception> E defunct(E e) {
        if (logger.isDebugEnabled())
            logger.debug("Defuncting connection to " + address, e);
        isDefunct = true;

        ConnectionException ce = e instanceof ConnectionException
                ? (ConnectionException) e
                : new ConnectionException(address, e, "Connection problem");

        Host host = factory.nettyManager.getHost(address);

        if (host != null) {
            // This will trigger onDown, including when the defunct Connection is part of a reconnection attempt, which is redundant.
            // This is not too much of a problem since calling onDown on a node that is already down has no effect.
            factory.nettyManager.signalConnectionFailure(host, ce, host.wasJustAdded());
            notifyOwnerWhenDefunct(true);
        }

        // Force the connection to close to make sure the future completes. Otherwise force() might never get called and
        // threads will wait on the future forever.
        // (this also errors out pending handlers)
        closeAsync().force();

        return e;
    }

    public CloseFuture closeAsync() {
        ConnectionCloseFuture future = new ConnectionCloseFuture();
        if (!closeFuture.compareAndSet(null, future)) {
            return closeFuture.get();
        }

        logger.debug("{} closing connection", this);

        boolean terminated = tryTerminate(false);
        if (!terminated) {
            // The time by which all pending requests should have normally completed (use twice the read timeout for a generous
            // estimate -- note that this does not cover the eventuality that read timeout is updated dynamically, but we can live
            // with that).
            long terminateTime = System.currentTimeMillis() + 2 * factory.getReadTimeoutMillis();
            factory.nettyManager.reaper.register(this, terminateTime);
        }

        return future;
    }

    boolean tryTerminate(boolean force) {
        assert isClosed();
        ConnectionCloseFuture future = closeFuture.get();

        if (future.isDone()) {
            logger.debug("{} has already terminated", this);
            return true;
        } else {
            if (force || dispatcher.pending.isEmpty()) {
                if (force)
                    logger.warn("Forcing termination of {}. This should not happen and is likely a bug, please report.", this);
                future.force();
                return true;
            } else {
                logger.debug("Not terminating {}: there are still pending requests", this);
                return false;
            }
        }
    }

    boolean isClosed() {
        return closeFuture.get() != null;
    }

    private ChannelFutureListener writeHandler(final Request request, final ResponseHandler handler) {
        return new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                writer.decrementAndGet();
                if (!future.isSuccess()) {
                    logger.debug("{} Error writing request {}", Connection.this, request);
                    // Remove this handler from the dispatcher so it don't get notified of the error
                    // twice (we will fail that method already)
                    dispatcher.removeHandler(handler, true);

                    final ConnectionException ce;
                    if (future.cause() instanceof ClosedChannelException) {
                        ce = new TransportException(address, "Error writing: Closed channel");
                    } else {
                        ce = new TransportException(address, future.cause(), "Error writing");
                    }

                    factory.nettyManager.executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            handler.callback.onException(Connection.this, defunct(ce), handler.retryCount);
                        }
                    });
                } else {
                    logger.trace("{} request sent successfully", Connection.this);
                }
            }
        };
    }

    @Override
    public String toString() {
        return String.format("Connection[%s, %s, closed=%b]", name, channel == null ? "" : channel.localAddress(), isClosed());
    }

    public interface ResponseCallback<T extends IRequest, V extends IResponse> {
        public void sendRequest(RequestHandler.ResultSetCallback callback);

        public int retryCount();

        public T request();

        public void onSet(Connection connection, V response, int retryCount);

        public void onException(Connection connection, Exception exception, int retryCount);

        public boolean onTimeout(Connection connection, int streamId, int retryCount);


    }

    static class ResponseHandler {
        public final Connection connection;
        public final int streamId;
        public final ResponseCallback callback;
        private final int retryCount;

        private final long startTime;
        private final AtomicBoolean isCancelled = new AtomicBoolean();
        private volatile boolean timeoutCancelled = false;
        private volatile Timeout timeout;

        ResponseHandler(Connection connection, ResponseCallback callback) {
            this.connection = connection;
            this.callback = callback;
            this.streamId = connection.dispatcher.streamIdHandler.next();
            this.retryCount = callback.retryCount();

            this.startTime = System.nanoTime();
        }

        void startTimeout() {
            int timeoutMs = connection.factory.getReadTimeoutMillis();
            if (timeoutCancelled) {
                return;
            }
            this.timeout = timeoutMs <= 0 ? null : connection.factory.timer.newTimeout(onTimeoutTask(), timeoutMs, TimeUnit.MILLISECONDS);
            if (timeoutCancelled) {
                timeout.cancel();
            }
        }

        void cancelTimeout() {
            // timeout maybe null if the response arrives before the return of startTimeout
            timeoutCancelled = true;
            if (timeout != null) {
                timeout.cancel();
            }
        }

        public void cancelHandler() {
            if (!isCancelled.compareAndSet(false, true))
                return;

            connection.dispatcher.removeHandler(this, false);
            if (connection instanceof PooledConnection)
                ((PooledConnection) connection).release();
        }

        private TimerTask onTimeoutTask() {
            return new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    if (callback.onTimeout(connection, streamId, retryCount))
                        cancelHandler();
                }
            };
        }
    }

    private static class Initializer extends ChannelInitializer {
        private final Connection connection;
        private final IPipelineAndHeartbeat pipelineAndHearbeat;

        private Initializer(Connection connection, IPipelineAndHeartbeat pipelineAndHearbeat) {
            this.connection = connection;
            this.pipelineAndHearbeat = pipelineAndHearbeat;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();

            pipelineAndHearbeat.configurePipeline(pipeline);

            int readTimeoutMillis = connection.factory.getReadTimeoutMillis();
            pipeline.addLast("idleStateHandler", new IdleStateHandler(readTimeoutMillis, readTimeoutMillis / 2, 0, TimeUnit.MILLISECONDS));

            pipeline.addLast(connection.dispatcher);
        }
    }

    static class Factory<T extends IRequest, V extends IResponse> {
        public final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("Timeouter-%d").build());

        final NettyManager nettyManager;
        final Configuration configuration;
        final IPipelineAndHeartbeat<T, V> pipelineAndHearbeat;
        private final ConcurrentMap<Host, AtomicInteger> idGenerators = new ConcurrentHashMap<Host, AtomicInteger>();
        private volatile boolean isShutdown;
        private EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        public Factory(NettyManager nettyManager, IPipelineAndHeartbeat<T, V> pipelineAndHearbeat, Configuration configuration) {
            this.nettyManager = nettyManager;
            this.pipelineAndHearbeat = pipelineAndHearbeat;
            this.configuration = configuration;
        }

        public Connection open(Host host) throws ConnectionException, InterruptedException {
            InetSocketAddress address = host.getAddress();

            if (isShutdown)
                throw new ConnectionException(address, "Connection factory is shutdown");

            String name = host.getAddress() + "-" + getIdGenerator(host).getAndIncrement();
            return new Connection<T, V>(name, address, pipelineAndHearbeat, this);
        }

        public PooledConnection open(HostConnectionPool pool) throws ConnectionException, InterruptedException {
            InetSocketAddress address = pool.host.getAddress();

            if (isShutdown)
                throw new ConnectionException(address, "Connection factory is shut down");

            String name = pool.host.getAddress() + "-" + getIdGenerator(pool.host).getAndIncrement();
            return new PooledConnection<T, V>(name, address, pipelineAndHearbeat, this, pool);
        }

        private AtomicInteger getIdGenerator(Host host) {
            AtomicInteger g = idGenerators.get(host);
            if (g == null) {
                g = new AtomicInteger();
                AtomicInteger old = idGenerators.putIfAbsent(host, g);
                if (old != null)
                    g = old;
            }
            return g;
        }

        private Bootstrap newBootstrap() {
            Bootstrap b = new Bootstrap();

            SocketOptions options = configuration.getSocketOptions();
            b.option(ChannelOption.ALLOCATOR, CBUtil.allocator);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.getConnectionTimeoutMillis());
            if (options.isKeepAlive() != null) {
                b.option(ChannelOption.SO_KEEPALIVE, options.isKeepAlive());
            } else {
                b.option(ChannelOption.SO_KEEPALIVE, true);
            }
            if (options.isReuseAddress() != null) {
                b.option(ChannelOption.SO_REUSEADDR, options.isReuseAddress());
            }
            if (options.getSoLinger() != null) {
                b.option(ChannelOption.SO_LINGER, options.getSoLinger());
            }
            if (options.isTcpNoDelay() != null) {
                b.option(ChannelOption.TCP_NODELAY, options.isTcpNoDelay());
            } else {
                b.option(ChannelOption.TCP_NODELAY, true);
            }
            if (options.getRecvBufferSize() != null) {
                b.option(ChannelOption.SO_RCVBUF, options.getRecvBufferSize());
            }
            if (options.getSendBufferSize() != null) {
                b.option(ChannelOption.SO_SNDBUF, options.getSendBufferSize());
            }

            b.group(eventLoopGroup).channel(NioSocketChannel.class);

            return b;
        }

        public int getReadTimeoutMillis() {
            return configuration.getSocketOptions().getReadTimeoutMillis();
        }

        public void shutDown() {
            // Make sure we skip creating connection from now on.
            isShutdown = true;
            eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
            timer.stop();
        }
    }

    private class ConnectionCloseFuture extends CloseFuture {
        @Override
        public CloseFuture force() {
            // This method can be thrown during Connection ctor, at which point channel is not yet set. This is ok.
            if (channel == null) {
                set(null);
                return this;
            }

            // We're going to close this channel. If anyone is waiting on that connection, we should defunct it otherwise it'll wait
            // forever. In general this won't happen since we get there only when all ongoing query are done, but this can happen
            // if the shutdown is forced. This is a no-op if there is no handler set anymore.
            dispatcher.errorOutAllHandler(new TransportException(address, "Connection has been closed"));

            ChannelFuture future = channel.close();
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.cause() != null) {
                        logger.warn("Error closing channel", future.cause());
                        ConnectionCloseFuture.this.setException(future.cause());
                    } else {
                        ConnectionCloseFuture.this.set(null);
                    }
                }
            });
            return this;
        }
    }

    @ChannelHandler.Sharable
    class Dispatcher extends SimpleChannelInboundHandler<IResponse> {
        public final StreamIdGenerator streamIdHandler;
        private final ConcurrentMap<Integer, ResponseHandler> pending = new ConcurrentHashMap<Integer, ResponseHandler>();

        public Dispatcher() {
            this.streamIdHandler = StreamIdGenerator.newInstance();
        }

        public void removeHandler(ResponseHandler handler, boolean releaseStreamId) {
            boolean removed = pending.remove(handler.streamId, handler);
            if (!removed) {
                logger.debug("handler {} has been removed for connection {}", handler.streamId, Connection.this);
                return;
            }

            handler.cancelTimeout();

            if (isClosed())
                tryTerminate(false);
        }

        public void add(ResponseHandler handler) {
            ResponseHandler old = pending.put(handler.streamId, handler);
            assert old == null;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, IResponse msg) throws Exception {
            int streamId = msg.getStreamId();
            logger.trace("{} received: {}", Connection.this, msg);

            ResponseHandler handler = pending.remove(streamId);

            if (handler == null) {
                /**
                 * During normal operation, we should not receive responses for which we don't have a handler. There is
                 * two cases however where this can happen:
                 *   1) The connection has been defuncted due to some internal error and we've raced between removing the
                 *      handler and actually closing the connection; since the original error has been logged, we're fine
                 *      ignoring this completely.
                 *   2) This request has timed out. In that case, we've already switched to another host (or errored out
                 *      to the user). So log it for debugging purpose, but it's fine ignoring otherwise.
                 */
                if (logger.isDebugEnabled()) {
                    logger.debug("{} response received on stream {}, but no handler set anymore", Connection.this, streamId);
                }
                return;
            }

            handler.cancelTimeout();
            handler.callback.onSet(Connection.this, msg, handler.retryCount);

            // If we happen to be closed and we're the last outstanding request, we need to terminate the connection
            // (note: this is racy as the signaling can be called more than once, but that's not a problem)
            if (isClosed())
                tryTerminate(false);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                logger.debug("{} was inactive", Connection.this);
                IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
                if (idleStateEvent.state() == IdleState.WRITER_IDLE) {
                    ResponseCallback<Request, Response> heartbeat = Connection.this.pipelineAndHearbeat.getHeartbeat();
                    if (heartbeat == null) {
                        logger.warn("pipelineAndHeartbeat has no heartbeat implementation");
                        return;
                    }
                    write(heartbeat, true);
                } else {
                    defunct(new TransportException(address, "Timed out receiving any data from server"));
                }
            }
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            logger.debug("client {} unregistered", Connection.this);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            // If we've closed the channel client side then we don't really want to defunct the connection, but
            // if there is remaining thread waiting on us, we still want to wake them up
            logger.debug("client {} inactive", Connection.this);
            if (!isInitialized || isClosed()) {
                errorOutAllHandler(new TransportException(address, "Channel has been closed"));
                // we still want to force so that the future completes
                Connection.this.closeAsync().force();
            } else {
                defunct(new TransportException(address, "Channel has been closed"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("%s connection error", Connection.this), cause);
            }

            if (writer.get() > 0) {
                return;
            }

            defunct(new TransportException(address, cause.getCause(), String.format("Unexpected exception triggered (%s)", cause)));
        }

        public void errorOutAllHandler(ConnectionException ce) {
            Iterator<ResponseHandler> iter = pending.values().iterator();
            while (iter.hasNext()) {
                ResponseHandler handler = iter.next();
                handler.cancelTimeout();
                handler.callback.onException(Connection.this, ce, handler.retryCount);
                iter.remove();
            }
        }
    }
}
