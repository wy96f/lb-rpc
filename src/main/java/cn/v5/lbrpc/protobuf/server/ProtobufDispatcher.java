package cn.v5.lbrpc.protobuf.server;

import cn.v5.lbrpc.protobuf.data.ProtobufMessage;
import cn.v5.lbrpc.protobuf.data.ProtobufRequestMeta;
import cn.v5.lbrpc.protobuf.data.ProtobufResponseMeta;
import cn.v5.lbrpc.protobuf.utils.ErrCodes;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangwei on 15-5-8.
 */
@ChannelHandler.Sharable
public class ProtobufDispatcher extends SimpleChannelInboundHandler<ProtobufMessage> {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufDispatcher.class);

    private final RpcServiceRegistry rpcServiceRegistry;

    public ProtobufDispatcher(RpcServiceRegistry rpcServiceRegistry) {
        super();
        this.rpcServiceRegistry = rpcServiceRegistry;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ProtobufMessage request) throws Exception {
        ProtobufMessage response;

        try {
            logger.trace("Received: {}", request);
            response = doExecute(request);
        } catch (Throwable t) {
            logger.warn("execute request " + request + " occurs exception: ", t.getCause());
            response = buildErrMsg(ErrCodes.ST_ERROR, t.getCause().toString());
        }

        response.setStreamId(request.getStreamId());

        logger.trace("Responding: {}", response);
        // TODO add listener
        ctx.writeAndFlush(response);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        logger.debug("server channel {} unregistered", ctx.channel().remoteAddress());

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("server channel {} inactive", ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) throws Exception {
        logger.debug("caught exception from {}: {}", ctx.channel().remoteAddress(), t);
    }

    private ProtobufMessage doExecute(ProtobufMessage request) throws Exception {
        String serviceName = request.getRequest().getSerivceName();
        String methodName = request.getRequest().getMethodName();

        RpcHandler handler =
                rpcServiceRegistry.lookupService(serviceName, methodName);

        if (handler == null) {
            ProtobufMessage message;
            message = dealwithHeartbeat(request);
            if (message != null) {
                return message;
            }
            return buildErrMsg(ErrCodes.ST_SERVICE_NOTFOUND, String.format(ErrCodes.MSG_SERVICE_NOTFOUND,
                    serviceName + "!" + methodName));
        } else {
            ProtobufMessage message = handler.execute(request);
            message.setResponse(new ProtobufResponseMeta(ErrCodes.ST_SUCCESS, null));
            return message;
        }
    }

    private ProtobufMessage dealwithHeartbeat(ProtobufMessage request) {
        if (request.getRequest().getSerivceName().compareTo(ProtobufRequestMeta.HEARTBEAT_SERVICE) == 0 &&
                request.getRequest().getMethodName().compareTo(ProtobufRequestMeta.HEARTBEAT_METHOD) == 0) {
            ProtobufMessage message = new ProtobufMessage();
            ProtobufResponseMeta responseMeta = new ProtobufResponseMeta(ErrCodes.ST_HEARBEAT, ProtobufRequestMeta.HEARTBEAT_SERVICE);
            message.setResponse(responseMeta);
            return message;
        }
        return null;
    }

    private ProtobufMessage buildErrMsg(int errCode, String errMsg) {
        ProtobufMessage msg = new ProtobufMessage();
        ProtobufResponseMeta responseMeta = new ProtobufResponseMeta(errCode, errMsg);
        msg.setResponse(responseMeta);
        return msg;
    }
}
