package cn.v5.lbrpc.common.server;

import cn.v5.lbrpc.common.client.core.ServerOptions;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import cn.v5.lbrpc.common.utils.CBUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.Version;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yangwei on 15-6-16.
 */
public abstract class AbstractRpcServer implements LifeCycleServer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRpcServer.class);
    private static final boolean enableEpoll = Boolean.valueOf(System.getProperty("cassandra.native.epoll.enabled", "true"));

    public final InetSocketAddress socket;
    public final ServiceRegistration registration;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    protected EventLoopGroup workerGroup;
    protected EventLoopGroup bossGroup;
    protected EventExecutorGroup group;
    private ServerOptions options;

    public AbstractRpcServer(InetSocketAddress socket, ServerOptions options, ServiceRegistration registration) {
        this.options = options;
        this.group = new DefaultEventExecutorGroup(options.getMax_threads(), new ThreadFactoryBuilder().setNameFormat("Rpc-Group-%d").build());
        this.socket = socket;
        this.registration = registration;
    }

    public AbstractRpcServer(InetSocketAddress socket, ServiceRegistration registration) {
        this(socket, new ServerOptions(), registration);
    }

    public AbstractRpcServer(int port, ServiceRegistration registration) {
        this(new InetSocketAddress(port), registration);
    }

    @Override
    public void start() {
        if (!isRunning()) {
            run();
        }
    }

    @Override
    public void stop() {
        if (isRunning.compareAndSet(true, false))
            close();
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    private void run() {
        boolean hasEpoll = enableEpoll ? Epoll.isAvailable() : false;
        if (hasEpoll) {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
            logger.info("Netty using native Epoll event loop");
        } else {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
            logger.info("Netty using NIO event loop");
        }

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(hasEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
                .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
                .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);

        bootstrap.childHandler(getChannelInitializer());

        // Bind and start to accept incoming connections.
        logger.info("Using Netty Version: {}", Version.identify().entrySet());
        logger.info("starting listening for rpc clients on {}...", socket);

        ChannelFuture bindFuture = bootstrap.bind(socket);
        if (!bindFuture.awaitUninterruptibly().isSuccess())
            throw new IllegalStateException(String.format("Failed to bind port %d on %s.", socket.getPort(), socket.getAddress()));

        isRunning.set(true);
    }

    protected abstract ChannelInitializer getChannelInitializer();

    private void close() {
        logger.info("starting stopping netty group on {}", socket);
        bossGroup.shutdownGracefully();
        group.shutdownGracefully();
        workerGroup.shutdownGracefully();
        try {
            bossGroup.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("stopping netty boss group of {} interrupted", socket);
        }
        bossGroup = null;
        group = null;
        workerGroup = null;
        logger.info("Stop listening on {}", socket);
    }
}
