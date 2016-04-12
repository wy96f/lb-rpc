package cn.v5.lbrpc.protobuf.server;

import cn.v5.lbrpc.common.client.core.ServerOptions;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import cn.v5.lbrpc.common.server.AbstractRpcServer;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.protobuf.data.ProtobufFrame;
import cn.v5.lbrpc.protobuf.data.ProtobufMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-5-8.
 */
public class ProtobufRpcServer extends AbstractRpcServer {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufRpcServer.class);
    private final RpcServiceRegistry serviceRegistry;

    public ProtobufRpcServer(InetSocketAddress socket, ServerOptions options, ServiceRegistration registration) {
        super(socket, options, registration);
        this.serviceRegistry = new RpcServiceRegistry(this);
    }

    public ProtobufRpcServer(InetSocketAddress socket, ServiceRegistration registration) {
        this(socket, new ServerOptions(), registration);
    }

    public ProtobufRpcServer(int port, ServiceRegistration registration) {
        this(new InetSocketAddress(port), registration);
    }

    @Override
    public String getProto() {
        return AbstractServerFactory.PROTOBUF_RPC;
    }

    @Override
    public void register(final Object target, String contextPath) {
        serviceRegistry.registerService(target);
    }

    @Override
    public void unregister() {
        serviceRegistry.unregisterServices();
    }

    @Override
    protected ChannelInitializer getChannelInitializer() {
        return new Initializer(group, serviceRegistry);
    }

    private static class Initializer extends ChannelInitializer {
        private static final ProtobufMessage.Encoder messageEncoder = new ProtobufMessage.Encoder();
        private static final ProtobufMessage.Decoder messageDecoder = new ProtobufMessage.Decoder();
        private static final ProtobufFrame.Encoder frameEncoder = new ProtobufFrame.Encoder();
        private final ProtobufDispatcher protobufDispatcher;
        private final EventExecutorGroup group;

        private Initializer(EventExecutorGroup group, RpcServiceRegistry registry) {
            this.group = group;
            this.protobufDispatcher = new ProtobufDispatcher(registry);
        }


        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();

            pipeline.addLast("frameDecoder", new ProtobufFrame.Decoder());
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("messageEncoder", messageEncoder);
            pipeline.addLast("messageDecoder", messageDecoder);

            pipeline.addLast(group, "dispatcher", protobufDispatcher);
        }
    }
}
