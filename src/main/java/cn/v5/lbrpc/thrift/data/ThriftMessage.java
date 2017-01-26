package cn.v5.lbrpc.thrift.data;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.DefaultResultFuture;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.data.Readable;
import cn.v5.lbrpc.common.data.Writerable;
import cn.v5.lbrpc.common.utils.CBUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Created by yangwei on 15-6-23.
 */
public class ThriftMessage implements Readable, Writerable, Cloneable, IResponse, IRequest {
    private static final Logger logger = LoggerFactory.getLogger(ThriftMessage.class);
    TApplicationException x;
    private String serviceName;
    private String methodName;
    private byte msgType;
    private int streamId;
    private TBase args;
    private byte[] result;
    private Map<String, String> header;
    private Object[] methodArgs;

    public ThriftMessage() {

    }

    public ThriftMessage(String serviceName, String methodName, Object[] args, byte msgType) {
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.msgType = msgType;
        this.methodArgs = args;
    }

    @Override
    public void read(byte[] bytes) {
        TTransport transport = new TMemoryInputTransport(bytes);
        TProtocol protocol = new TMultiplexedProtocol(new TBinaryProtocol(transport), serviceName);

        try {
            TMessage msg = protocol.readMessageBegin();
            streamId = msg.seqid;
            if (msg.type == TMessageType.EXCEPTION) {
                x = TApplicationException.read(protocol);
                protocol.readMessageEnd();
                return;
            }

            int offset = transport.getBufferPosition();
            result = Arrays.copyOfRange(bytes, offset, bytes.length);
/*            if (msg.seqid != streamId) {
                x = new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, methodName + " failed: out of sequence response");
                return;
            }*/
        } catch (TException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public String getService() {
        return serviceName;
    }

    @Override
    public String getMethod() {
        return methodName;
    }

    @Override
    public int getStreamId() {
        return streamId;
    }

    @Override
    public void setStreamId(int id) {
        this.streamId = id;
    }

    @Override
    public boolean onResponse(Connection connection, DefaultResultFuture resultFuture) throws IOException {
        if (x != null) {
            resultFuture.setException(new RpcException(String.format("Unexpected error occurred server side on %s",
                        connection.address), x));
        } else {
            AbstractRpcMethodInfo abstractRpcMethodInfo = resultFuture.getAbstractRpcMethodInfo();
            Object res = abstractRpcMethodInfo.outputDecode(result);
            if (res instanceof Throwable) {
                resultFuture.setException(new RpcException((Throwable)res));
            } else {
                resultFuture.set(res);
            }
        }
        return false;
    }

    @Override
    public byte[] write() {
        TMemoryBuffer transport = new TMemoryBuffer(128);
        TProtocol protocol = new TMultiplexedProtocol(new TBinaryProtocol(transport), serviceName);

        try {
            protocol.writeMessageBegin(new TMessage(methodName, msgType, streamId));

            args.write(protocol);
            protocol.writeMessageEnd();
            protocol.getTransport().flush();
        } catch (TException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return Arrays.copyOfRange(transport.getArray(), 0, transport.length());
        //return transport.getArray();
    }

    public void setArgs(TBase args) {
        this.args = args;
    }

    @Override
    public void setHeader(String key, String value) {
        if (header == null) header = new HashMap<>();
        header.put(key, value);
    }

    @Override
    public Object[] getArgs() {
        return methodArgs;
    }

    @Override
    public String toString() {
        if (msgType == TMessageType.CALL) {
            return String.format("Request %s.%s for %d", serviceName, methodName, getStreamId());
        } else {
            return String.format("Response %d", msgType, getStreamId());
        }
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<ThriftMessage> {
        @Override
        protected void encode(ChannelHandlerContext ctx, ThriftMessage msg, List<Object> out) throws Exception {
            byte[] encodedMsg = msg.write();
            ByteBuf body = CBUtil.allocator.buffer(encodedMsg.length);
            body.writeBytes(encodedMsg);

            out.add(new ThriftFrame(body, msg.header));
        }
    }

    @ChannelHandler.Sharable
    public static class Decoder extends MessageToMessageDecoder<ThriftFrame> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ThriftFrame frame, List<Object> out) throws Exception {
            try {
                ThriftMessage message = new ThriftMessage();
                byte[] body = new byte[frame.body.readableBytes()];
                frame.body.readBytes(body);
                message.read(body);
                if (frame.getHeader() != null) {
                    for (String key : frame.getHeader().keySet()) {
                        message.setHeader(key, frame.getHeader().get(key));
                    }
                }
                out.add(message);
            } finally {
                // must release msg here
                frame.release();
            }
        }
    }
}
