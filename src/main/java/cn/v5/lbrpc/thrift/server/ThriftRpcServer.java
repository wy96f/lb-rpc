package cn.v5.lbrpc.thrift.server;

import cn.v5.lbrpc.common.client.core.ServerOptions;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import cn.v5.lbrpc.common.server.AbstractRpcServer;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.thrift.data.HeartbeatInternal;
import cn.v5.lbrpc.thrift.data.ThriftFrame;
import cn.v5.lbrpc.thrift.utils.ThriftUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yangwei on 15-6-16.
 */
public class ThriftRpcServer extends AbstractRpcServer {
    private static final Logger logger = LoggerFactory.getLogger(ThriftRpcServer.class);
    private final static HeartbeatInternal.Iface heartbeatInternalImpl = new HeartbeatInternal.Iface() {
        @Override
        public void heartbeat() throws TException {
            // nothing to do
        }
    };
    private final Initializer initializer;
    private final Set<String> services = Sets.newCopyOnWriteArraySet();

    public ThriftRpcServer(InetSocketAddress socket, ServerOptions options, ServiceRegistration registration) {
        super(socket, options, registration);
        this.initializer = new Initializer(group);
        try {
            ThriftUtils.ProcessorInfo processorInfo = ThriftUtils.getProcessor(heartbeatInternalImpl);
            initializer.addProcessor(processorInfo.getServiceName(), processorInfo.getProcessor());
        } catch (Exception e) {
            logger.warn("add heartbeat processor err: {}", e);
        }
    }

    public ThriftRpcServer(InetSocketAddress socket, ServiceRegistration registration) {
        super(socket, registration);
        this.initializer = new Initializer(group);
        try {
            ThriftUtils.ProcessorInfo processorInfo = ThriftUtils.getProcessor(heartbeatInternalImpl);
            initializer.addProcessor(processorInfo.getServiceName(), processorInfo.getProcessor());
        } catch (Exception e) {
            logger.warn("add heartbeat processor err: {}", e);
        }
    }

    public ThriftRpcServer(int port, ServiceRegistration registration) {
        super(port, registration);
        this.initializer = new Initializer(group);
        try {
            ThriftUtils.ProcessorInfo processorInfo = ThriftUtils.getProcessor(heartbeatInternalImpl);
            initializer.addProcessor(processorInfo.getServiceName(), processorInfo.getProcessor());
        } catch (Exception e) {
            logger.warn("add heartbeat processor err: {}", e);
        }
    }

    @Override
    protected ChannelInitializer getChannelInitializer() {
        return this.initializer;
    }

    @Override
    public String getProto() {
        return AbstractServerFactory.THRIFT_RPC;
    }

    @Override
    public void register(Object instance, String contextPath) throws Exception {
        ThriftUtils.ProcessorInfo processorInfo = ThriftUtils.getProcessor(instance);
        initializer.addProcessor(processorInfo.getServiceName(), processorInfo.getProcessor());
        registration.registerServer(processorInfo.getServiceName(), CBUtil.THRIFT_PROTO, socket);
        services.add(processorInfo.getServiceName());
    }

    @Override
    public void unregister() {
        List<ListenableFuture<?>> futureList = Lists.newArrayList();
        for (String service : services) {
            ListenableFuture<?> unregisterFuture = registration.unregisterServer(service, CBUtil.THRIFT_PROTO, socket);
            if (unregisterFuture != null) {
                futureList.add(unregisterFuture);
            }
        }
        try {
            Futures.successfulAsList(futureList).get(4000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Unexpected error while unregistering thrift services", e.getCause());
        } catch (TimeoutException e) {
            logger.warn("Time out while unregistering thrift services", e);
        }
    }

    private static class Initializer extends ChannelInitializer {
        private static final ThriftFrame.Encoder frameEncoder = new ThriftFrame.Encoder();
        private final EventExecutorGroup group;
        ThriftDispatcher dispatcher = new ThriftDispatcher();

        private Initializer(EventExecutorGroup group) {
            this.group = group;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("frameDecoder", new ThriftFrame.Decoder(102400, 0, 4));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast(group, "dispatcher", dispatcher);
        }

        protected void addProcessor(String serviceName, TProcessor processor) {
            dispatcher.addProcessor(serviceName, processor);
        }
    }
}