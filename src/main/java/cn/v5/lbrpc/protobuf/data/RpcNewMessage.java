package cn.v5.lbrpc.protobuf.data;

import cn.v5.lbrpc.common.data.Writerable;
import cn.v5.lbrpc.common.utils.CBUtil;
import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.io.IOException;
import java.util.List;

/**
 * Created by yangwei on 15-5-4.
 */
public class RpcNewMessage implements cn.v5.lbrpc.common.data.Readable, Writerable, Cloneable {
    public static final RpcNewMessage HEART_BEAT =
            new RpcNewMessage(new ProtobufRequestMeta(ProtobufRequestMeta.HEARTBEAT_SERVICE, ProtobufRequestMeta.HEARTBEAT_METHOD));
    /**
     * Decode and encode handler
     */
    private static final Codec<RpcNewMessage> CODEC = ProtobufProxy.create(RpcNewMessage.class);
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
    private Long streamId;

    @Protobuf(fieldType = FieldType.OBJECT)
    private List<Parameter> parameters;

    public RpcNewMessage() {
    }

    public RpcNewMessage(ProtobufRequestMeta request) {
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
            RpcNewMessage message = CODEC.decode(bytes);
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

    public Long getStreamId() {
        return streamId;
    }

    public void setStreamId(Long streamId) {
        this.streamId = streamId;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * copy {@link cn.v5.lbrpc.protobuf.data.RpcNewMessage}
     *
     * @param message
     */
    private void copyReference(RpcNewMessage message) {
        if (message == null) {
            return;
        }
        setRequest(message.getRequest());
        setResponse(message.getResponse());
        setStreamId(message.getStreamId());
        setParameters(message.getParameters());
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
    public static class Encoder extends MessageToMessageEncoder<RpcNewMessage> {
        @Override
        protected void encode(ChannelHandlerContext ctx, RpcNewMessage msg, List<Object> out) throws Exception {
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
                RpcNewMessage message = new RpcNewMessage();
                byte[] body = new byte[msg.body.readableBytes()];
                msg.body.readBytes(body);
                message.read(body);
                out.add(message);
            } finally {
                msg.release();
            }
        }
    }
}
