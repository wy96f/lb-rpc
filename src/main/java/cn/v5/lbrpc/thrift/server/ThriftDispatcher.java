package cn.v5.lbrpc.thrift.server;

import cn.v5.lbrpc.common.server.ServerInterceptor;
import cn.v5.lbrpc.thrift.data.ThriftFrame;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Created by yangwei on 15-6-16.
 */
@ChannelHandler.Sharable
public class ThriftDispatcher extends SimpleChannelInboundHandler<ThriftFrame> {
    private static final Logger logger = LoggerFactory.getLogger(ThriftDispatcher.class);
    protected final TMultiplexedProcessor multiplexedProcessor;
    protected final TProtocolFactory protocolFactory;
    protected final List<ServerInterceptor> interceptors;

    public ThriftDispatcher(List<ServerInterceptor> interceptors) {
        this.interceptors = interceptors;
        this.multiplexedProcessor = new TMultiplexedProcessor();
        this.protocolFactory = new TBinaryProtocol.Factory();
    }

    protected void addProcessor(String serviceName, TProcessor processor) {
        multiplexedProcessor.registerProcessor(serviceName, processor);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error(String.format("%s occurs exception",ctx.channel().remoteAddress()), cause);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ThriftFrame msg) throws Exception {
        TChannelBufferTransport transport = new TChannelBufferTransport(msg.body, ctx.channel());
        TProtocol inProtocol = protocolFactory.getProtocol(transport);
        TProtocol outProtocol = protocolFactory.getProtocol(transport);

        processRequest(ctx, msg, transport, inProtocol, outProtocol);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleEvent = (IdleStateEvent)evt;
            if (idleEvent.state() == IdleState.READER_IDLE) {
                logger.info("{} has been read idle for a while, close it", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    private void preProcess(TMessage tMessage, SocketAddress addr, Map<String, String> header) {
        if (interceptors == null || interceptors.isEmpty()) return;
        for (ServerInterceptor interceptor : interceptors) {
            interceptor.preProcess(tMessage.name, addr, header);
        }
    }

    private void postProcess(Exception e) {
        if (interceptors == null || interceptors.isEmpty()) return;
        for (ServerInterceptor interceptor : interceptors) {
            interceptor.postProcess(e);
        }
    }

    private void processRequest(ChannelHandlerContext ctx, ThriftFrame msg, TChannelBufferTransport transport, TProtocol inProtocol, TProtocol outProtocol) {
        TProtocol decoratedProtocol = null;
        try {
            /*
            Use the actual underlying protocol (e.g. TBinaryProtocol) to read the
            message header.  This pulls the message "off the wire", which we'll
            deal with at the end of this method.
        */
            TMessage message = inProtocol.readMessageBegin();

            if (message.type != TMessageType.CALL && message.type != TMessageType.ONEWAY) {
                throw new TException("This should not have happened!?");
            }

            // Create a new TMessage, removing the service name
            TMessage standardMessage = new TMessage(
                    message.name,
                    message.type,
                    message.seqid
            );

            decoratedProtocol = new StoredMessageProtocol(inProtocol, standardMessage);


            preProcess(standardMessage, ctx.channel().remoteAddress(), msg.getHeader());
            multiplexedProcessor.process(decoratedProtocol, outProtocol);
            postProcess(null);
            writeResponse(ctx, new ThriftFrame(transport.getOutputBuffer(), null));
        } catch (TException e) {
            logger.error(ctx.channel().remoteAddress() + " occurs texception: ", e);
            if (e.getMessage().contains("Service name not found")) {
                TApplicationException applicationException = new TApplicationException(TApplicationException.UNKNOWN_METHOD, e.getMessage());
                sendTApplicationException(ctx, applicationException, transport, decoratedProtocol, outProtocol);
            } else {
                closeChannel(ctx);
            }
        } catch (Throwable e) {
            logger.error(ctx.channel().remoteAddress() + " occurs error: ", e);
            TApplicationException applicationException = new TApplicationException(TApplicationException.INTERNAL_ERROR, e.toString());
            sendTApplicationException(ctx, applicationException, transport, decoratedProtocol, outProtocol);
        } finally {
            msg.release();
        }
    }

    private void closeChannel(ChannelHandlerContext ctx) {
        if (ctx.channel().isOpen()) {
            ctx.close();
        }
    }

    private void writeResponse(ChannelHandlerContext ctx, ThriftFrame response) {
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    private void sendTApplicationException(ChannelHandlerContext ctx, TApplicationException x, TChannelBufferTransport transport, TProtocol inProtocol, TProtocol outProtocol) {
        try {
            TMessage message = inProtocol.readMessageBegin();
            // Extract the service name
            int index = message.name.indexOf(TMultiplexedProtocol.SEPARATOR);
            // Create a new TMessage, something that can be consumed by any TProtocol
            String serviceName = message.name.substring(0, index);

            outProtocol.writeMessageBegin(new TMessage(message.name.substring(serviceName.length() + TMultiplexedProtocol.SEPARATOR.length()),
                    TMessageType.EXCEPTION, message.seqid));
            x.write(outProtocol);
            outProtocol.writeMessageEnd();
            outProtocol.getTransport().flush();

            postProcess(x);
            writeResponse(ctx, new ThriftFrame(transport.getOutputBuffer(), null));
        } catch (TTransportException e) {
            logger.error(ctx.channel().remoteAddress() + " occurs error on sending exception: ", e);
            closeChannel(ctx);
        } catch (TException e) {
            logger.error(ctx.channel().remoteAddress() + " occurs error on sending exception: ", e);
            closeChannel(ctx);
        }
    }

    //
    private static class StoredMessageProtocol extends TProtocolDecorator {
        TMessage messageBegin;

        public StoredMessageProtocol(TProtocol protocol, TMessage messageBegin) {
            super(protocol);
            this.messageBegin = messageBegin;
        }

        @Override
        public TMessage readMessageBegin() throws TException {
            return messageBegin;
        }
    }
}
