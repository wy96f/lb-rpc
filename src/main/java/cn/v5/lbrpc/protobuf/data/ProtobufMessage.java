package cn.v5.lbrpc.protobuf.data;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.DefaultResultFuture;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.data.Readable;
import cn.v5.lbrpc.common.data.Writerable;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.protobuf.utils.ErrCodes;
import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by yangwei on 15-5-4.
 */
public class ProtobufMessage implements Readable, Writerable, Cloneable, IResponse, IRequest {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufMessage.class);

    public static final ProtobufMessage HEART_BEAT =
            new ProtobufMessage(new ProtobufRequestMeta(ProtobufRequestMeta.HEARTBEAT_SERVICE, ProtobufRequestMeta.HEARTBEAT_METHOD));
    /**
     * Decode and encode handler
     */
    private static final Codec<ProtobufMessage> CODEC = ProtobufProxy.create(ProtobufMessage.class);
    /**
     * 请求包元数据
     */
    @Protobuf(fieldType = FieldType.OBJECT)
    private ProtobufRequestMeta request;

    /**
     * 响应包元数据
     */
    @Protobuf(fieldType = FieldType.OBJECT)
    private ProtobufResponseMeta response;

    /**
     * 请求包中的该域由请求方设置，用于唯一标识一个RPC请求。<br>
     * 请求方有义务保证其唯一性，协议本身对此不做任何检查。<br>
     * 响应方需要在对应的响应包里面将correlation_id设为同样的值。
     */
    @Protobuf(required = true)
    private Integer streamId;

    @Protobuf(fieldType = FieldType.BYTES)
    private byte[] data;

    public ProtobufMessage() {
    }

    public ProtobufMessage(ProtobufRequestMeta request) {
        this.request = request;
    }

    /* (non-Javadoc)
        * @see com.baidu.jprotobuf.remoting.pbrpc.Writerable#write()
    */
    public byte[] write() {
        try {
            return CODEC.encode(this);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /* (non-Javadoc)
     * @see com.baidu.jprotobuf.remoting.pbrpc.Readable#read(byte[])
     */
    public void read(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("param 'bytes' is null.");
        }
        try {
            ProtobufMessage message = CODEC.decode(bytes);
            copyReference(message);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public ProtobufRequestMeta getRequest() {
        return request;
    }

    public void setRequest(ProtobufRequestMeta request) {
        this.request = request;
    }

    public ProtobufResponseMeta getResponse() {
        return response;
    }

    public void setResponse(ProtobufResponseMeta response) {
        this.response = response;
    }

    @Override
    public int getStreamId() {
        return streamId;
    }

    @Override
    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }

    @Override
    public boolean onResponse(Connection connection, DefaultResultFuture resultFuture) throws IOException {
        if (ErrCodes.isSuccess(response.getErrorCode())) {
            AbstractRpcMethodInfo abstractRpcMethodInfo = resultFuture.getAbstractRpcMethodInfo();
            resultFuture.set(abstractRpcMethodInfo.outputDecode(data));
        } else if (ErrCodes.ST_SERVICE_NOTFOUND == response.getErrorCode().intValue()) {
            logger.warn("{}, doing retry", response.getErrorText());
            resultFuture.getHandler().logError(connection.address, new RpcException(response.getErrorText()));
            return true;
        } else {
            resultFuture.setException(new RpcException(String.format("Unexpected error occurred server side on %s: %d %s",
                    connection.address, response.getErrorCode(), response.getErrorText())));
        }
        return false;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * copy {@link ProtobufMessage}
     *
     * @param message
     */
    private void copyReference(ProtobufMessage message) {
        if (message == null) {
            return;
        }
        setRequest(message.getRequest());
        setResponse(message.getResponse());
        setStreamId(message.getStreamId());
        setData(message.getData());
    }

    @Override
    public String toString() {
        if (request != null) {
            return String.format("Request %s.%s for %d", request.getSerivceName(), request.getMethodName(), getStreamId());
        } else {
            return String.format("Response %d,%s for %d", response.getErrorCode(), response.getErrorText(), getStreamId());
        }
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<ProtobufMessage> {
        @Override
        protected void encode(ChannelHandlerContext ctx, ProtobufMessage msg, List<Object> out) throws Exception {
            byte[] encodedMsg = msg.write();
            ByteBuf body = CBUtil.allocator.buffer(encodedMsg.length);
            body.writeBytes(encodedMsg);

            out.add(new ProtobufFrame(new ProtobufFrame.Header(ProtobufFrame.CURRENT_VERSION, body.readableBytes()), body));
        }
    }

    @ChannelHandler.Sharable
    public static class Decoder extends MessageToMessageDecoder<ProtobufFrame> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ProtobufFrame msg, List<Object> out) throws Exception {
            try {
                ProtobufMessage message = new ProtobufMessage();
                byte[] body = new byte[msg.body.readableBytes()];
                msg.body.readBytes(body);
                message.read(body);
                out.add(message);
            } finally {
                // must release msg here
                msg.release();
            }
        }
    }
}
